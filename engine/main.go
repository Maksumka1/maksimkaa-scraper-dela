package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const filterSchemaVersion = 2

type UserFilter struct {
	ChatID int64

	// Legacy fields. They remain supported for zero-downtime rollout and old /set filters.
	RouteFrom string
	RouteTo   string

	FromAllUkraine bool
	FromCities     []string
	FromRegions    []string
	ToAllUkraine   bool
	ToCities       []string
	ToRegions      []string

	MinWeight     float64
	MaxWeight     float64
	MinVolume     float64
	MaxVolume     float64
	MinPricePerKm float64

	TransportTypes      []string
	ReturnSearchEnabled bool
	RoundTripOnly       bool
	Enabled             bool
}

type FilterStore struct {
	sync.RWMutex
	filters map[int64]*UserFilter
}

func NewFilterStore() *FilterStore {
	return &FilterStore{filters: make(map[int64]*UserFilter)}
}

func (fs *FilterStore) Set(filter *UserFilter) {
	fs.Lock()
	defer fs.Unlock()
	if !filter.Enabled {
		delete(fs.filters, filter.ChatID)
		return
	}
	fs.filters[filter.ChatID] = filter
}

func (fs *FilterStore) Delete(chatID int64) {
	fs.Lock()
	defer fs.Unlock()
	delete(fs.filters, chatID)
}

func (fs *FilterStore) GetAll() []*UserFilter {
	fs.RLock()
	defer fs.RUnlock()
	list := make([]*UserFilter, 0, len(fs.filters))
	for _, f := range fs.filters {
		list = append(list, f)
	}
	return list
}

type CargoPayload struct {
	RequestID         string
	RouteFrom         string
	RouteTo           string
	RouteFromFull     string
	RouteToFull       string
	RouteFromRegion   string
	RouteToRegion     string
	CargoType         string
	Tags              []string
	TransportTypes    []string
	DistanceKm        int
	WeightT           float64
	VolumeM3          float64
	PriceUAH          float64
	PricePerKmUAH     float64
	PublishedRelative string
}

type TelegramTask struct {
	ChatID int64
	Text   string
}

type TelegramAPIResponse struct {
	Ok          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func parseStringSlice(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		log.Printf("Некоректний JSON-масив фільтра: %v", err)
		return nil
	}

	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = normalizeFilterValue(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func parseFloatField(data map[string]string, key string) float64 {
	value := strings.TrimSpace(data[key])
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func parseBoolField(data map[string]string, key string) bool {
	value := strings.ToLower(strings.TrimSpace(data[key]))
	return value == "1" || value == "true" || value == "yes"
}

func normalizeFilterValue(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func parsePayload(vals map[string]interface{}) (*CargoPayload, error) {
	getString := func(k string) string {
		if v, ok := vals[k].(string); ok {
			return v
		}
		return ""
	}

	dist, _ := strconv.Atoi(getString("distance_km"))
	weight, _ := strconv.ParseFloat(getString("weight_t"), 64)
	vol, _ := strconv.ParseFloat(getString("volume_m3"), 64)
	price, _ := strconv.ParseFloat(getString("price_uah"), 64)
	priceKm, _ := strconv.ParseFloat(getString("price_per_km_uah"), 64)

	return &CargoPayload{
		RequestID:         getString("request_id"),
		RouteFrom:         getString("route_from"),
		RouteTo:           getString("route_to"),
		RouteFromFull:     getString("route_from_full"),
		RouteToFull:       getString("route_to_full"),
		RouteFromRegion:   getString("route_from_region"),
		RouteToRegion:     getString("route_to_region"),
		CargoType:         getString("cargo_type"),
		Tags:              parseStringSlice(getString("tags")),
		TransportTypes:    parseStringSlice(getString("transport_types")),
		DistanceKm:        dist,
		WeightT:           weight,
		VolumeM3:          vol,
		PriceUAH:          price,
		PricePerKmUAH:     priceKm,
		PublishedRelative: getString("published_relative"),
	}, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisAddr := os.Getenv("REDIS_ADDR")
	redisPass := os.Getenv("REDIS_PASSWORD")
	dbURL := os.Getenv("DATABASE_URL")
	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPass})
	defer rdb.Close()

	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Помилка підключення до PostgreSQL: %v", err)
	}
	defer dbPool.Close()

	store := NewFilterStore()
	loadFilters(ctx, rdb, store)
	go watchFilterUpdates(ctx, rdb, store)

	go startDataRetentionWorker(ctx, dbPool)

	tgQueue := make(chan TelegramTask, 5000)
	var wg sync.WaitGroup

	// Диспетчер з розпаралелюванням за шардами (Sharded Worker Pool)
	wg.Add(1)
	go startTelegramDispatcher(ctx, &wg, tgQueue, botToken, rdb, store)

	streamKey := "stream:della:requests"
	groupName := "engine_group"
	consumerName := "engine-worker"
	_ = rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "$").Err()

	log.Println("🚀 Production Engine успішно запущено.")

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				entries, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    groupName,
					Consumer: consumerName,
					Streams:  []string{streamKey, ">"},
					Count:    50,
					Block:    2 * time.Second,
				}).Result()

				if err != nil {
					if err != redis.Nil {
						log.Printf("Помилка Redis Stream: %v. Пауза 1с...", err)
						time.Sleep(1 * time.Second)
					}
					continue
				}

				if len(entries) == 0 {
					continue
				}

				var toSave []*CargoPayload
				var pendingMsgIDs []string
				var immediateAckIDs []string

				for _, msg := range entries[0].Messages {
					cargo, err := parsePayload(msg.Values)
					if err != nil || cargo.RequestID == "" {
						// A malformed record cannot be processed successfully later.
						immediateAckIDs = append(immediateAckIDs, msg.ID)
						continue
					}

					seenKey := fmt.Sprintf("cargo:seen:%s", cargo.RequestID)
					exists, err := rdb.Exists(ctx, seenKey).Result()
					if err != nil {
						log.Printf("Помилка перевірки dedup Redis для %s: %v", cargo.RequestID, err)
						continue
					}
					if exists > 0 {
						immediateAckIDs = append(immediateAckIDs, msg.ID)
						continue
					}

					toSave = append(toSave, cargo)
					pendingMsgIDs = append(pendingMsgIDs, msg.ID)
				}

				if len(immediateAckIDs) > 0 {
					if err := rdb.XAck(ctx, streamKey, groupName, immediateAckIDs...).Err(); err != nil {
						log.Printf("Помилка XAck для вже оброблених записів: %v", err)
					}
				}

				if len(toSave) == 0 {
					continue
				}

				if err := saveBatchToPostgres(ctx, dbPool, toSave); err != nil {
					// Do not ACK/mark seen: Redis Stream will redeliver the records.
					log.Printf("Помилка запису в Postgres: %v. Повідомлення залишено для повторної обробки", err)
					continue
				}

				ackAfterSave := make([]string, 0, len(pendingMsgIDs))
				for i, cargo := range toSave {
					seenKey := fmt.Sprintf("cargo:seen:%s", cargo.RequestID)
					wasSet, err := rdb.SetNX(ctx, seenKey, 1, 48*time.Hour).Result()
					if err != nil {
						log.Printf("Не вдалося позначити cargo=%s як seen: %v. Повторна обробка дозволена", cargo.RequestID, err)
						continue
					}
					ackAfterSave = append(ackAfterSave, pendingMsgIDs[i])
					if !wasSet {
						continue
					}

					for _, f := range store.GetAll() {
						if !match(cargo, f) {
							continue
						}

						text := formatAlert(cargo)
						var returnCandidates []*CargoPayload
						if f.ReturnSearchEnabled || f.RoundTripOnly {
							candidates, err := findReturnCargo(ctx, dbPool, cargo, f, 5)
							if err != nil {
								log.Printf("Помилка пошуку зворотного вантажу для %s: %v", cargo.RequestID, err)
								continue
							}
							if f.RoundTripOnly {
								returnCandidates, err = recordNewRoundTripPairs(ctx, dbPool, f.ChatID, cargo.RequestID, candidates)
								if err != nil {
									log.Printf("Помилка запису round-trip pair для %s/%d: %v", cargo.RequestID, f.ChatID, err)
									continue
								}
								if len(returnCandidates) == 0 {
									continue
								}
								text = formatRoundTripAlert(cargo, returnCandidates)
							} else {
								returnCandidates = candidates
								text += formatReturnCandidates(returnCandidates)
							}
						}

						select {
						case tgQueue <- TelegramTask{ChatID: f.ChatID, Text: text}:
						default:
							log.Printf("Головна черга переповнена, пропуск для %d", f.ChatID)
						}
					}
				}

				if len(ackAfterSave) > 0 {
					if err := rdb.XAck(ctx, streamKey, groupName, ackAfterSave...).Err(); err != nil {
						log.Printf("Помилка XAck після збереження: %v", err)
					}
				}

			}
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Отримано сигнал зупинки. Безпечне завершення...")
	cancel()
	close(tgQueue) // Закриття вхідної черги; time.AfterFunc більше немає, паніка виключена
	wg.Wait()
	log.Println("Engine успішно зупинено.")
}

func startTelegramDispatcher(
	ctx context.Context,
	parentWg *sync.WaitGroup,
	inQueue <-chan TelegramTask,
	token string,
	rdb *redis.Client,
	store *FilterStore,
) {
	defer parentWg.Done()

	const numWorkers = 8
	workerQueues := make([]chan TelegramTask, numWorkers)
	var workersWg sync.WaitGroup

	// Спільний глобальний rate limiter: 25 повідомлень/сек
	globalLimiter := time.NewTicker(40 * time.Millisecond)
	defer globalLimiter.Stop()

	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)

	// Запуск шардованих воркерів
	for i := 0; i < numWorkers; i++ {
		workerQueues[i] = make(chan TelegramTask, 500)
		workersWg.Add(1)

		go func(wQueue <-chan TelegramTask) {
			defer workersWg.Done()
			lastSentPerChat := make(map[int64]time.Time)

			for task := range wQueue {
				// 1. Індивідуальний ліміт на користувача (1 повідомлення / 1.1 сек)
				if lastSent, exists := lastSentPerChat[task.ChatID]; exists {
					elapsed := time.Since(lastSent)
					if elapsed < 1100*time.Millisecond {
						waitDuration := 1100*time.Millisecond - elapsed
						timer := time.NewTimer(waitDuration)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
					}
				}

				// 2. Глобальний ліміт API (потокобезпечне споживання з єдиного каналу)
				select {
				case <-ctx.Done():
					return
				case <-globalLimiter.C:
				}

				lastSentPerChat[task.ChatID] = time.Now()

				sendHTTPRequest(ctx, client, url, task, rdb, store)
			}
		}(workerQueues[i])
	}

	// Маршрутизація повідомлень у відповідний шард за ChatID
	for task := range inQueue {
		shardIdx := int(task.ChatID % numWorkers)
		if shardIdx < 0 {
			shardIdx = -shardIdx
		}

		select {
		case workerQueues[shardIdx] <- task:
		default:
			log.Printf("Шард %d переповнений! Пропуск сповіщення для %d", shardIdx, task.ChatID)
		}
	}

	// Закриваємо черги воркерів після вичерпання inQueue
	for i := 0; i < numWorkers; i++ {
		close(workerQueues[i])
	}
	workersWg.Wait()
}

func sendHTTPRequest(
	ctx context.Context,
	client *http.Client,
	url string,
	task TelegramTask,
	rdb *redis.Client,
	store *FilterStore,
) {
	body, _ := json.Marshal(map[string]interface{}{
		"chat_id":    task.ChatID,
		"text":       task.Text,
		"parse_mode": "HTML",
	})

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Помилка відправки HTTP в Telegram: %v", err)
		return
	}

	respBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return
	}

	var apiResp TelegramAPIResponse
	_ = json.Unmarshal(respBytes, &apiResp)

	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		retrySec := apiResp.Parameters.RetryAfter
		if retrySec <= 0 {
			retrySec = 1
		}
		log.Printf("Глобальний 429 ліміт Telegram! Очікування %d сек", retrySec)
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(retrySec) * time.Second):
		}

	case http.StatusForbidden, http.StatusBadRequest:
		if strings.Contains(apiResp.Description, "bot was blocked") ||
			strings.Contains(apiResp.Description, "chat not found") {
			log.Printf("Користувач %d недоступний. Видалення фільтра...", task.ChatID)
			store.Delete(task.ChatID)

			pipe := rdb.Pipeline()
			pipe.Del(ctx, fmt.Sprintf("filter:%d", task.ChatID))
			pipe.SRem(ctx, "filters:active_users", strconv.FormatInt(task.ChatID, 10))
			pipe.Publish(ctx, "channel:filters:update", strconv.FormatInt(task.ChatID, 10))
			_, _ = pipe.Exec(ctx)
		}
	}
}

func historyRetentionDays() int {
	const defaultRetentionDays = 180
	raw := strings.TrimSpace(os.Getenv("CARGO_HISTORY_RETENTION_DAYS"))
	if raw == "" {
		return defaultRetentionDays
	}
	days, err := strconv.Atoi(raw)
	if err != nil || days < 48/24 || days > 3650 {
		log.Printf("Некоректний CARGO_HISTORY_RETENTION_DAYS=%q; використовую %d", raw, defaultRetentionDays)
		return defaultRetentionDays
	}
	return days
}

func startDataRetentionWorker(ctx context.Context, db *pgxpool.Pool) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	retentionDays := historyRetentionDays()
	batchQuery := `
		WITH to_delete AS (
			SELECT request_id FROM cargo_history
			WHERE created_at < $1
			LIMIT 1000
		)
		DELETE FROM cargo_history
		WHERE request_id IN (SELECT request_id FROM to_delete);
	`
	pairCleanupQuery := `
		DELETE FROM round_trip_pairs
		WHERE created_at < $1;
	`

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour)
			totalDeleted := int64(0)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				res, err := db.Exec(ctx, batchQuery, cutoff)
				if err != nil {
					log.Printf("Помилка батч-очищення: %v", err)
					break
				}

				deleted := res.RowsAffected()
				totalDeleted += deleted

				if deleted < 1000 {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			if totalDeleted > 0 {
				log.Printf("Очищення історії: видалено %d застарілих записів (retention=%d днів)", totalDeleted, retentionDays)
			}
			if _, err := db.Exec(ctx, pairCleanupQuery, cutoff); err != nil {
				log.Printf("Помилка очищення round-trip pairs: %v", err)
			}
		}
	}
}

func saveBatchToPostgres(ctx context.Context, db *pgxpool.Pool, items []*CargoPayload) error {
	batch := &pgx.Batch{}
	query := `
		INSERT INTO cargo_history (
			request_id, route_from, route_to, route_from_full, route_to_full,
			route_from_region, route_to_region, cargo_type, tags, transport_types,
			distance_km, weight_t, volume_m3, price_uah, price_per_km_uah, published_relative
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (request_id) DO UPDATE SET
			route_from_full = EXCLUDED.route_from_full,
			route_to_full = EXCLUDED.route_to_full,
			route_from_region = EXCLUDED.route_from_region,
			route_to_region = EXCLUDED.route_to_region,
			tags = EXCLUDED.tags,
			transport_types = EXCLUDED.transport_types;
	`

	for _, c := range items {
		batch.Queue(query,
			c.RequestID,
			c.RouteFrom,
			c.RouteTo,
			c.RouteFromFull,
			c.RouteToFull,
			c.RouteFromRegion,
			c.RouteToRegion,
			c.CargoType,
			c.Tags,
			c.TransportTypes,
			c.DistanceKm,
			c.WeightT,
			c.VolumeM3,
			c.PriceUAH,
			c.PricePerKmUAH,
			c.PublishedRelative,
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for range items {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}

	// geo_locations is intentionally maintained by the Go persistence layer.
	// The scraper remains responsible only for extracting source data.
	geoBatch := &pgx.Batch{}
	geoQuery := `
		INSERT INTO geo_locations (city_name, district_name, region_name)
		VALUES ($1, NULLIF($2, ''), $3)
		ON CONFLICT (city_name, region_name) DO UPDATE SET
			district_name = COALESCE(EXCLUDED.district_name, geo_locations.district_name),
			updated_at = NOW();
	`

	type geoKey struct {
		city   string
		region string
	}
	seenGeo := make(map[geoKey]struct{})
	for _, c := range items {
		addGeo := func(city, full, region string) {
			city = strings.TrimSpace(city)
			region = strings.TrimSpace(region)
			if city == "" || region == "" {
				return
			}

			district := ""
			if comma := strings.Index(full, ","); comma > 0 {
				district = strings.TrimSpace(full[:comma])
			}

			key := geoKey{city: normalizeFilterValue(city), region: normalizeFilterValue(region)}
			if _, exists := seenGeo[key]; exists {
				return
			}
			seenGeo[key] = struct{}{}
			geoBatch.Queue(geoQuery, city, district, region)
		}

		addGeo(c.RouteFrom, c.RouteFromFull, c.RouteFromRegion)
		addGeo(c.RouteTo, c.RouteToFull, c.RouteToRegion)
	}

	if len(seenGeo) > 0 {
		geoResult := db.SendBatch(ctx, geoBatch)
		defer geoResult.Close()
		for range seenGeo {
			if _, err := geoResult.Exec(); err != nil {
				return err
			}
		}
	}

	return nil
}

func match(c *CargoPayload, f *UserFilter) bool {
	if !locationMatches(
		c.RouteFrom,
		c.RouteFromFull,
		c.RouteFromRegion,
		f.FromAllUkraine,
		f.FromCities,
		f.FromRegions,
		f.RouteFrom,
	) {
		return false
	}

	if !locationMatches(
		c.RouteTo,
		c.RouteToFull,
		c.RouteToRegion,
		f.ToAllUkraine,
		f.ToCities,
		f.ToRegions,
		f.RouteTo,
	) {
		return false
	}

	if f.MinWeight > 0 && c.WeightT < f.MinWeight {
		return false
	}
	if f.MaxWeight > 0 {
		if c.WeightT <= 0 || c.WeightT > f.MaxWeight {
			return false
		}
	}
	if f.MinVolume > 0 && c.VolumeM3 < f.MinVolume {
		return false
	}
	if f.MaxVolume > 0 {
		if c.VolumeM3 <= 0 || c.VolumeM3 > f.MaxVolume {
			return false
		}
	}
	if f.MinPricePerKm > 0 && c.PricePerKmUAH < f.MinPricePerKm {
		return false
	}
	if len(f.TransportTypes) > 0 && !hasTransportIntersection(c.TransportTypes, f.TransportTypes) {
		return false
	}

	return true
}

func locationMatches(
	city string,
	full string,
	region string,
	allUkraine bool,
	cities []string,
	regions []string,
	legacy string,
) bool {
	if allUkraine {
		return true
	}

	if len(cities) == 0 && len(regions) == 0 && legacy == "" {
		return true
	}

	normalizedCity := normalizeFilterValue(city)
	normalizedFull := normalizeFilterValue(full)
	normalizedRegion := normalizeFilterValue(region)

	for _, target := range cities {
		if normalizedCity == normalizeFilterValue(target) {
			return true
		}
	}

	for _, target := range regions {
		normalizedTarget := normalizeFilterValue(target)
		if normalizedRegion == normalizedTarget {
			return true
		}
		// Defensive fallback for old cargo rows where only route_*_full exists.
		if normalizedTarget != "" && strings.Contains(normalizedFull, normalizedTarget) {
			return true
		}
	}

	// Backward compatibility with the original scalar substring matching.
	if legacy != "" && strings.Contains(normalizedCity, normalizeFilterValue(legacy)) {
		return true
	}

	return false
}

func hasTransportIntersection(cargoTypes, filterTypes []string) bool {
	cargoSet := make(map[string]struct{}, len(cargoTypes))
	for _, value := range cargoTypes {
		cargoSet[normalizeFilterValue(value)] = struct{}{}
	}
	for _, value := range filterTypes {
		if _, ok := cargoSet[normalizeFilterValue(value)]; ok {
			return true
		}
	}
	return false
}

func formatAlert(c *CargoPayload) string {
	text := fmt.Sprintf(
		"⚡️ <b>Новий вантаж!</b>\n\n"+
			"📍 <b>%s ➔ %s</b> (%d км)\n"+
			"Вантаж: %s | %.1f т | %.1f м³\n"+
			"💰 <b>%.0f грн</b> (<b>%.2f грн/км</b>)\n",
		escapeTelegramHTML(c.RouteFrom), escapeTelegramHTML(c.RouteTo), c.DistanceKm,
		escapeTelegramHTML(c.CargoType), c.WeightT, c.VolumeM3,
		c.PriceUAH, c.PricePerKmUAH,
	)

	if len(c.TransportTypes) > 0 {
		text += fmt.Sprintf("🚛 Транспорт: %s\n", escapeTelegramHTML(strings.Join(c.TransportTypes, ", ")))
	}
	text += fmt.Sprintf("⏱ %s", escapeTelegramHTML(c.PublishedRelative))
	return text
}

func buildUserFilter(chatID int64, data map[string]string) *UserFilter {
	if rawVersion := strings.TrimSpace(data["schema_version"]); rawVersion != "" {
		version, err := strconv.Atoi(rawVersion)
		if err == nil && version > filterSchemaVersion {
			log.Printf("Фільтр %d має новішу schema_version=%d; застосовано відомі поля", chatID, version)
		}
	}

	filter := &UserFilter{
		ChatID:              chatID,
		RouteFrom:           normalizeFilterValue(data["route_from"]),
		RouteTo:             normalizeFilterValue(data["route_to"]),
		FromAllUkraine:      parseBoolField(data, "from_all_ukraine"),
		FromCities:          parseStringSlice(data["from_cities"]),
		FromRegions:         parseStringSlice(data["from_regions"]),
		ToAllUkraine:        parseBoolField(data, "to_all_ukraine"),
		ToCities:            parseStringSlice(data["to_cities"]),
		ToRegions:           parseStringSlice(data["to_regions"]),
		MinWeight:           parseFloatField(data, "min_weight"),
		MaxWeight:           parseFloatField(data, "max_weight"),
		MinVolume:           parseFloatField(data, "min_volume"),
		MaxVolume:           parseFloatField(data, "max_volume"),
		MinPricePerKm:       parseFloatField(data, "min_price_km"),
		TransportTypes:      parseStringSlice(data["transport_types"]),
		ReturnSearchEnabled: parseBoolField(data, "return_search_enabled"),
		RoundTripOnly:       parseBoolField(data, "round_trip_only"),
		Enabled:             parseBoolField(data, "enabled"),
	}

	return filter
}

func loadFilters(ctx context.Context, rdb *redis.Client, store *FilterStore) {
	users, err := rdb.SMembers(ctx, "filters:active_users").Result()
	if err != nil {
		return
	}
	for _, chatIDStr := range users {
		chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
		if err != nil {
			continue
		}
		data, err := rdb.HGetAll(ctx, fmt.Sprintf("filter:%d", chatID)).Result()
		if err == nil && len(data) > 0 {
			store.Set(buildUserFilter(chatID, data))
		}
	}
}

func watchFilterUpdates(ctx context.Context, rdb *redis.Client, store *FilterStore) {
	pubsub := rdb.Subscribe(ctx, "channel:filters:update")
	defer pubsub.Close()
	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			chatID, err := strconv.ParseInt(msg.Payload, 10, 64)
			if err != nil {
				continue
			}
			data, err := rdb.HGetAll(ctx, fmt.Sprintf("filter:%d", chatID)).Result()
			if err != nil || len(data) == 0 {
				store.Delete(chatID)
			} else {
				store.Set(buildUserFilter(chatID, data))
			}
		}
	}
}
