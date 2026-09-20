import json
import logging
import os
import random
import re
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urlparse

from curl_cffi import requests
from curl_cffi.requests.exceptions import RequestException
from lxml import html
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("DellaParser")

# Ініціалізація підключення до Redis із параметрів оточення
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")
REDIS_PASS = os.getenv("REDIS_PASSWORD")

host, port = REDIS_ADDR.split(":")
redis_client = redis.Redis(
    host=host,
    port=int(port),
    password=REDIS_PASS,
    decode_responses=True,
)


@dataclass
class CargoRequest:
    request_id: str
    dateup_timestamp: int
    published_relative: str
    route_from: str
    route_to: str
    distance_km: Optional[int]
    cargo_type: str
    weight_t: Optional[float]
    volume_m3: Optional[float]
    price_uah: Optional[int]
    price_per_km_uah: Optional[float]
    tags: List[str] = field(default_factory=list)
    parsed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_redis_payload(self) -> Dict[str, str]:
        data = asdict(self)
        data["tags"] = json.dumps(data["tags"], ensure_ascii=False)
        return {k: str(v) if v is not None else "" for k, v in data.items()}


@dataclass
class ParserMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    parse_failures: int = 0
    rate_limited_count: int = 0
    failed_requests: int = 0
    items_extracted: int = 0
    unique_items_seen: int = 0
    avg_response_time_ms: float = 0.0
    current_interval_sec: float = 0.0
    empty_results: int = 0
    structural_anomalies: int = 0


class AdaptiveRateLimiter:
    def __init__(
        self,
        min_interval: float = 3.0,
        max_interval: float = 60.0,
        initial_interval: float = 10.0,
        backoff_multiplier: float = 2.5,
    ):
        if min_interval <= 0 or max_interval < min_interval:
            raise ValueError("Некоректні min/max interval")
        if not min_interval <= initial_interval <= max_interval:
            raise ValueError("initial_interval має бути між min_interval та max_interval")
        if backoff_multiplier <= 1:
            raise ValueError("backoff_multiplier має бути > 1")

        self.min_interval = min_interval
        self.max_interval = max_interval
        self.current_interval = initial_interval
        self.backoff_multiplier = backoff_multiplier

    def feedback(
        self,
        status_code: int,
        has_new_data: bool,
        is_blocked: bool,
        is_network_error: bool = False,
    ):
        if is_network_error:
            old = self.current_interval
            self.current_interval = min(
                self.current_interval * self.backoff_multiplier,
                self.max_interval,
            )
            logger.warning(
                f"[RateLimiter] Мережева помилка. "
                f"{old:.1f}s -> {self.current_interval:.1f}s"
            )
            return

        if is_blocked or status_code in (403, 429, 503):
            old = self.current_interval
            self.current_interval = min(
                self.current_interval * self.backoff_multiplier,
                self.max_interval,
            )
            logger.warning(
                f"[RateLimiter] Ліміт/блок HTTP {status_code}. "
                f"{old:.1f}s -> {self.current_interval:.1f}s"
            )
            return

        if has_new_data:
            old = self.current_interval
            self.current_interval = max(
                self.current_interval * 0.8,
                self.min_interval,
            )
            logger.info(
                f"[RateLimiter] Нові дані. "
                f"{old:.1f}s -> {self.current_interval:.1f}s"
            )
        else:
            old = self.current_interval
            self.current_interval = min(
                self.current_interval + 1.5,
                self.max_interval,
            )
            logger.debug(
                f"[RateLimiter] Нових даних немає. "
                f"{old:.1f}s -> {self.current_interval:.1f}s"
            )


class DellaMobileScraper:
    BASE_URL = "https://della.ua/search/a204bd204eflolh0ilk0m1.html"
    ALLOWED_HOSTS = {"della.ua", "www.della.ua"}

    def __init__(
        self,
        search_url: Optional[str] = None,
        seen_cache_size: int = 10000,
    ):
        self.target_url = search_url or self.BASE_URL
        self._validate_target_url(self.target_url)

        self.session = requests.Session(impersonate="chrome120")
        self.rate_limiter = AdaptiveRateLimiter()

        self._seen_ids = set()
        self._seen_order = deque(maxlen=seen_cache_size)

        self.metrics = ParserMetrics()
        self._total_latency = 0.0

        self.session.headers.update(
            {
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://della.ua/",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    @classmethod
    def _validate_target_url(cls, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "https":
            raise ValueError("target_url має використовувати HTTPS")
        if parsed.hostname not in cls.ALLOWED_HOSTS:
            raise ValueError(
                f"Недозволений host: {parsed.hostname!r}. "
                f"Дозволено тільки {sorted(cls.ALLOWED_HOSTS)}"
            )

    @staticmethod
    def _extract_clean_number(
        text: Optional[str],
        decimal: bool = True,
    ) -> Optional[float]:
        if not text:
            return None

        value = text.replace("\xa0", " ").strip()
        value = re.sub(r"[^\d,.\-\s]", "", value)
        value = re.sub(r"\s+", " ", value).strip()

        if not value:
            return None

        if "-" in value[1:]:
            return None

        negative = value.startswith("-")
        unsigned = value[1:] if negative else value
        unsigned = unsigned.replace(" ", "")

        if not unsigned or not re.fullmatch(r"\d+(?:[,.]\d+)*", unsigned):
            return None

        commas = unsigned.count(",")
        dots = unsigned.count(".")

        if commas and dots:
            decimal_sep = "," if unsigned.rfind(",") > unsigned.rfind(".") else "."
            grouping_sep = "." if decimal_sep == "," else ","
            unsigned = unsigned.replace(grouping_sep, "")
            unsigned = unsigned.replace(decimal_sep, ".", 1)
            if unsigned.count(".") > 1:
                return None
        elif commas:
            if commas > 1:
                parts = unsigned.split(",")
                if all(len(part) == 3 for part in parts[1:]):
                    unsigned = "".join(parts)
                else:
                    return None
            else:
                left, right = unsigned.split(",", 1)
                unsigned = f"{left}.{right}" if decimal else left + right
        elif dots:
            if dots > 1:
                parts = unsigned.split(".")
                if all(len(part) == 3 for part in parts[1:]):
                    unsigned = "".join(parts)
                else:
                    return None
            else:
                left, right = unsigned.split(".", 1)
                if len(right) == 3 and len(left) >= 1:
                    unsigned = left + right
                elif decimal:
                    unsigned = f"{left}.{right}"
                else:
                    unsigned = left + right

        try:
            result = float(unsigned)
            if negative:
                result = -result
        except ValueError:
            return None

        return result if result >= 0 else None

    @staticmethod
    def _node_text(node) -> str:
        return " ".join(" ".join(node.xpath(".//text()")).split())

    @staticmethod
    def _is_captcha_page(response) -> bool:
        if response.status_code == 200 and len(response.text) > 10000:
            return False

        body = response.text.lower()
        title_match = re.search(
            r"<title\b[^>]*>(.*?)</title\s*>",
            response.text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        title = " ".join(title_match.group(1).split()).lower() if title_match else ""

        block_titles = (
            "just a moment...",
            "attention required! | cloudflare",
            "security check",
            "ddos-guard",
        )
        if any(bt in title for bt in block_titles):
            return True

        if response.status_code in (403, 429):
            challenge_markers = (
                "captcha challenge",
                "verify you are human",
                "challenge-platform",
                "cf-chl-",
                "hcaptcha-response",
                "g-recaptcha-response",
            )
            return any(marker in body or marker in title for marker in challenge_markers)

        return False

    def _remember_ids(self, request_ids: List[str]) -> List[str]:
        new_ids = []

        for request_id in request_ids:
            if request_id in self._seen_ids:
                continue

            new_ids.append(request_id)
            if len(self._seen_order) == self._seen_order.maxlen:
                old_id = self._seen_order[0]
                self._seen_ids.discard(old_id)

            self._seen_order.append(request_id)
            self._seen_ids.add(request_id)

        return new_ids

    def _has_unseen_items(self, items: List[CargoRequest]) -> bool:
        batch_seen = set()
        for item in items:
            request_id = item.request_id
            if request_id in batch_seen or request_id in self._seen_ids:
                continue
            return True
        return False

    def _filter_new_items(self, items: List[CargoRequest]) -> List[CargoRequest]:
        new_items: List[CargoRequest] = []
        batch_seen = set()

        for item in items:
            request_id = item.request_id
            if request_id in batch_seen or request_id in self._seen_ids:
                continue
            batch_seen.add(request_id)
            new_items.append(item)

        self._remember_ids([item.request_id for item in new_items])
        return new_items

    @classmethod
    def _validate_response_host(cls, response) -> None:
        final_url = getattr(response, "url", None)
        if not final_url:
            return

        parsed = urlparse(final_url)
        if parsed.scheme != "https" or parsed.hostname not in cls.ALLOWED_HOSTS:
            raise ValueError(
                f"HTTP redirect/response веде на недозволений host: {final_url!r}"
            )

    def _validate_page(self, html_content: str) -> None:
        if not html_content or not html_content.strip():
            raise ValueError("Сервер повернув порожній HTML")

        error_markers = (
            "internal server error",
            "service unavailable",
            "bad gateway",
            "502 bad gateway",
        )

        title_matches = re.findall(
            r"<title\b[^>]*>(.*?)</title\s*>",
            html_content,
            flags=re.IGNORECASE | re.DOTALL,
        )
        h1_matches = re.findall(
            r"<h1\b[^>]*>(.*?)</h1\s*>",
            html_content,
            flags=re.IGNORECASE | re.DOTALL,
        )

        headline_parts = title_matches + h1_matches
        headline_text = " ".join(
            re.sub(r"<[^>]+>", " ", part)
            for part in headline_parts
        )
        headline_text = " ".join(headline_text.lower().split())

        if any(marker in headline_text for marker in error_markers):
            raise ValueError("HTML схожий на error page")

    def parse_html(self, html_content: str) -> List[CargoRequest]:
        self._validate_page(html_content)

        try:
            tree = html.fromstring(html_content)
        except Exception as exc:
            raise ValueError(f"Не вдалося розібрати HTML: {exc}") from exc

        cards = tree.xpath(
            '//div[contains(concat(" ", normalize-space(@class), " "), " request_card ")]'
        )

        if not cards:
            self.metrics.empty_results += 1
            self.metrics.structural_anomalies += 1
            logger.warning(
                "[Parser anomaly] HTTP 200, але не знайдено жодної request_card. "
                "Це може бути легітимно порожній результат або зміна DOM Della."
            )

        results: List[CargoRequest] = []

        for card in cards:
            req_id = card.get("data-request_id") or card.get("request_id")
            if not req_id:
                logger.debug("Пропущена картка без request_id")
                continue

            dateup_values = card.xpath(
                "./ancestor-or-self::*[@dateup][1]/@dateup"
            )
            dateup_raw = dateup_values[0].strip() if dateup_values else ""
            try:
                dateup_ts = int(dateup_raw)
            except (TypeError, ValueError):
                dateup_ts = 0
                logger.debug("Некоректний dateup для request_id=%s", req_id)

            time_node = card.xpath(
                './/div[contains(concat(" ", normalize-space(@class), " "), " time_string ")]//text()'
            )
            time_str = " ".join(" ".join(time_node).split()) if time_node else ""

            localities = card.xpath(
                './/div[contains(concat(" ", normalize-space(@class), " "), " request_route ")]'
                '//span[contains(concat(" ", normalize-space(@class), " "), " locality ")]//text()'
            )
            locality_values = [" ".join(x.split()) for x in localities if x.strip()]
            route_from = locality_values[0] if len(locality_values) > 0 else ""
            route_to = locality_values[1] if len(locality_values) > 1 else ""

            dist_node = card.xpath(
                './/a[contains(concat(" ", normalize-space(@class), " "), " distance ")]//text()'
            )
            dist_num = (
                self._extract_clean_number(" ".join(dist_node))
                if dist_node
                else None
            )
            dist_km = int(dist_num) if dist_num is not None else None

            weight_node = card.xpath(
                './/div[contains(concat(" ", normalize-space(@class), " "), " weight ")]//text()'
            )
            volume_node = card.xpath(
                './/div[contains(concat(" ", normalize-space(@class), " "), " cube ")]//text()'
            )
            weight_val = (
                self._extract_clean_number(" ".join(weight_node))
                if weight_node
                else None
            )
            volume_val = (
                self._extract_clean_number(" ".join(volume_node))
                if volume_node
                else None
            )

            cargo_nodes = card.xpath(
                './/span[contains(concat(" ", normalize-space(@class), " "), " cargo_type ")]'
            )
            cargo_desc = self._node_text(cargo_nodes[0]) if cargo_nodes else ""

            price_main_node = card.xpath(
                './/div[contains(concat(" ", normalize-space(@class), " "), " price_main ")]'
                '/text() | '
                './/div[contains(concat(" ", normalize-space(@class), " "), " price_main ")]'
                '/span/text()'
            )
            price_val = None
            if price_main_node:
                p_clean = self._extract_clean_number(" ".join(price_main_node))
                price_val = int(p_clean) if p_clean is not None else None

            price_km_node = card.xpath(
                './/div[contains(concat(" ", normalize-space(@class), " "), " price_additional ")]//text()'
            )
            price_km_val = (
                self._extract_clean_number(" ".join(price_km_node))
                if price_km_node
                else None
            )

            tag_nodes = (
                card.xpath(
                    './/div[contains(concat(" ", normalize-space(@class), " "), " price_tags ")]'
                    '//div[contains(concat(" ", normalize-space(@class), " "), " tag ")]//text()'
                )
                + card.xpath(
                    './/div[contains(concat(" ", normalize-space(@class), " "), " request_tags ")]'
                    '//div[contains(concat(" ", normalize-space(@class), " "), " tag ")]//text()'
                )
            )
            tags = [" ".join(t.split()) for t in tag_nodes if t.strip()]

            results.append(
                CargoRequest(
                    request_id=req_id,
                    dateup_timestamp=dateup_ts,
                    published_relative=time_str,
                    route_from=route_from,
                    route_to=route_to,
                    distance_km=dist_km,
                    cargo_type=cargo_desc,
                    weight_t=weight_val,
                    volume_m3=volume_val,
                    price_uah=price_val,
                    price_per_km_uah=price_km_val,
                    tags=tags,
                )
            )

        return results

    def _fetch_http_once(self) -> List[CargoRequest]:
        self.metrics.total_requests += 1
        t_start = time.perf_counter()

        try:
            resp = self.session.get(self.target_url, timeout=12)
        except RequestException as ex:
            self.metrics.failed_requests += 1
            logger.error(
                "[Network error] Не вдалося звернутися до %s: %s",
                self.target_url,
                ex,
            )
            self.rate_limiter.feedback(
                500, False, is_blocked=False, is_network_error=True
            )
            return []

        try:
            self._validate_response_host(resp)
        except ValueError as ex:
            self.metrics.failed_requests += 1
            logger.error("[Security] %s", ex)
            self.rate_limiter.feedback(500, False, is_blocked=True)
            return []

        latency_ms = (time.perf_counter() - t_start) * 1000
        self._total_latency += latency_ms
        self.metrics.avg_response_time_ms = (
            self._total_latency / self.metrics.total_requests
        )

        is_captcha = self._is_captcha_page(resp)
        is_blocked = resp.status_code in (403, 429, 503) or is_captcha

        if is_blocked:
            self.metrics.rate_limited_count += 1
            self.rate_limiter.feedback(
                resp.status_code,
                False,
                is_blocked=True,
            )
            logger.warning(
                "[Blocked/limited] HTTP %s, captcha=%s, latency=%.1fms",
                resp.status_code,
                is_captcha,
                latency_ms,
            )
            return []

        if resp.status_code != 200:
            self.metrics.failed_requests += 1
            logger.error(
                "Помилка отримання даних: HTTP %s",
                resp.status_code,
            )
            self.rate_limiter.feedback(
                resp.status_code,
                False,
                is_blocked=False,
            )
            return []

        try:
            items = self.parse_html(resp.text)
        except ValueError as ex:
            self.metrics.parse_failures += 1
            logger.exception("[Parser error] Некоректна відповідь Della: %s", ex)
            self.rate_limiter.feedback(200, False, is_blocked=False)
            return []
        except Exception:
            self.metrics.parse_failures += 1
            logger.exception("[Parser error] Неочікувана помилка під час parsing")
            self.rate_limiter.feedback(200, False, is_blocked=False)
            return []

        self.metrics.successful_requests += 1
        self.metrics.items_extracted += len(items)
        self.metrics.unique_items_seen = len(self._seen_ids)

        has_new_data = self._has_unseen_items(items)
        self.rate_limiter.feedback(
            resp.status_code,
            has_new_data,
            is_blocked=False,
        )
        self.metrics.current_interval_sec = self.rate_limiter.current_interval

        logger.info(
            "[Fetch OK] Отримано: %s заявок | "
            "Latency: %.1fms | Наст. пауза: %.1fs",
            len(items),
            latency_ms,
            self.rate_limiter.current_interval,
        )

        return items

    def fetch_once(self) -> List[CargoRequest]:
        return self._fetch_http_once()

    def fetch_new_once(self) -> List[CargoRequest]:
        items = self._fetch_http_once()
        new_items = self._filter_new_items(items)
        self.metrics.unique_items_seen = len(self._seen_ids)

        logger.info(
            "[Dedup] Snapshot: %s | Нових: %s | У кеші: %s",
            len(items),
            len(new_items),
            len(self._seen_ids),
        )
        return new_items

    def run_forever(self):
        logger.info("Della parser запущено. Target: %s", self.target_url)

        while True:
            started = time.monotonic()

            try:
                new_items = self.fetch_new_once()
                for item in new_items:
                    payload = item.to_redis_payload()
                    redis_client.xadd(
                        "stream:della:requests",
                        payload,
                        maxlen=20000,
                        approximate=True,
                    )
                    logger.info(f"[Stream Push] Вантаж {item.request_id} надіслано в Redis")
            except KeyboardInterrupt:
                logger.info("Зупинка парсера.")
                break
            except Exception:
                logger.exception("[Worker error] Неочікувана помилка циклу")

            elapsed = time.monotonic() - started
            target_interval = self.rate_limiter.current_interval * random.uniform(0.90, 1.10)
            delay = max(0.0, target_interval - elapsed)

            self.metrics.current_interval_sec = self.rate_limiter.current_interval

            if delay > 0:
                time.sleep(delay)


if __name__ == "__main__":
    scraper = DellaMobileScraper()
    scraper.run_forever()