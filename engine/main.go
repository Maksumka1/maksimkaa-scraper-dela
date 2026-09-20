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

type UserFilter struct {
	ChatID        int64
	RouteFrom     string
	RouteTo       string
	MinWeight     float64
	MinPricePerKm float64
	Enabled       bool
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
	CargoType         string
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
		CargoType:         getString("cargo_type"),
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
				var ackIDs []string

				for _, msg := range entries[0].Messages {
					ackIDs = append(ackIDs, msg.ID)
					cargo, err := parsePayload(msg.Values)
					if err != nil || cargo.RequestID == "" {
						continue
					}

					seenKey := fmt.Sprintf("cargo:seen:%s", cargo.RequestID)
					wasSet, _ := rdb.SetNX(ctx, seenKey, 1, 48*time.Hour).Result()
					if !wasSet {
						continue
					}

					toSave = append(toSave, cargo)

					for _, f := range store.GetAll() {
						if match(cargo, f) {
							select {
							case tgQueue <- TelegramTask{
								ChatID: f.ChatID,
								Text:   formatAlert(cargo),
							}:
							default:
								log.Printf("Головна черга переповнена, пропуск для %d", f.ChatID)
							}
						}
					}
				}

				if len(toSave) > 0 {
					if err := saveBatchToPostgres(ctx, dbPool, toSave); err != nil {
						log.Printf("Помилка запису в Postgres: %v", err)
					}
				}

				if len(ackIDs) > 0 {
					rdb.XAck(ctx, streamKey, groupName, ackIDs...)
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

func startDataRetentionWorker(ctx context.Context, db *pgxpool.Pool) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	batchQuery := `
		WITH to_delete AS (
			SELECT request_id FROM cargo_history
			WHERE created_at < NOW() - INTERVAL '48 HOURS'
			LIMIT 1000
		)
		DELETE FROM cargo_history
		WHERE request_id IN (SELECT request_id FROM to_delete);
	`

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			totalDeleted := int64(0)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				res, err := db.Exec(ctx, batchQuery)
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
				log.Printf("Очищення історії: видалено %d застарілих записів", totalDeleted)
			}
		}
	}
}

func saveBatchToPostgres(ctx context.Context, db *pgxpool.Pool, items []*CargoPayload) error {
	batch := &pgx.Batch{}
	query := `
		INSERT INTO cargo_history (
			request_id, route_from, route_to, cargo_type, distance_km, 
			weight_t, volume_m3, price_uah, price_per_km_uah, published_relative
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (request_id) DO NOTHING;
	`
	for _, c := range items {
		batch.Queue(query, c.RequestID, c.RouteFrom, c.RouteTo, c.CargoType, c.DistanceKm,
			c.WeightT, c.VolumeM3, c.PriceUAH, c.PricePerKmUAH, c.PublishedRelative)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for range items {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func match(c *CargoPayload, f *UserFilter) bool {
	if f.RouteFrom != "" && !strings.Contains(strings.ToLower(c.RouteFrom), f.RouteFrom) {
		return false
	}
	if f.RouteTo != "" && !strings.Contains(strings.ToLower(c.RouteTo), f.RouteTo) {
		return false
	}
	if f.MinWeight > 0 && c.WeightT < f.MinWeight {
		return false
	}
	if f.MinPricePerKm > 0 && c.PricePerKmUAH < f.MinPricePerKm {
		return false
	}
	return true
}

func formatAlert(c *CargoPayload) string {
	return fmt.Sprintf(
		"⚡️ <b>Новий вантаж!</b>\n\n"+
			"📍 <b>%s ➔ %s</b> (%d км)\n"+
			"Вантаж: %s | %.1f т | %.1f м³\n"+
			"💰 <b>%.0f грн</b> (<b>%.2f грн/км</b>)\n"+
			"⏱ %s",
		c.RouteFrom, c.RouteTo, c.DistanceKm,
		c.CargoType, c.WeightT, c.VolumeM3,
		c.PriceUAH, c.PricePerKmUAH,
		c.PublishedRelative,
	)
}

func loadFilters(ctx context.Context, rdb *redis.Client, store *FilterStore) {
	users, err := rdb.SMembers(ctx, "filters:active_users").Result()
	if err != nil {
		return
	}
	for _, chatIDStr := range users {
		chatID, _ := strconv.ParseInt(chatIDStr, 10, 64)
		data, err := rdb.HGetAll(ctx, fmt.Sprintf("filter:%d", chatID)).Result()
		if err == nil && len(data) > 0 {
			weight, _ := strconv.ParseFloat(data["min_weight"], 64)
			price, _ := strconv.ParseFloat(data["min_price_km"], 64)
			store.Set(&UserFilter{
				ChatID:        chatID,
				RouteFrom:     data["route_from"],
				RouteTo:       data["route_to"],
				MinWeight:     weight,
				MinPricePerKm: price,
				Enabled:       data["enabled"] == "1",
			})
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
		case msg := <-ch:
			chatID, _ := strconv.ParseInt(msg.Payload, 10, 64)
			data, err := rdb.HGetAll(ctx, fmt.Sprintf("filter:%d", chatID)).Result()
			if err != nil || len(data) == 0 {
				store.Delete(chatID)
			} else {
				weight, _ := strconv.ParseFloat(data["min_weight"], 64)
				price, _ := strconv.ParseFloat(data["min_price_km"], 64)
				store.Set(&UserFilter{
					ChatID:        chatID,
					RouteFrom:     data["route_from"],
					RouteTo:       data["route_to"],
					MinWeight:     weight,
					MinPricePerKm: price,
					Enabled:       data["enabled"] == "1",
				})
			}
		}
	}
}