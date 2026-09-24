package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// findReturnCargo searches the retained active history for cargo moving from
// the forward destination back toward the forward origin. Location matching
// is intentionally city OR region based, so an exact city pair or an
// equivalent regional route can be discovered without over-constraining the
// user to one locality.
func findReturnCargo(ctx context.Context, db *pgxpool.Pool, forward *CargoPayload, filter *UserFilter, limit int) ([]*CargoPayload, error) {
	if db == nil || forward == nil || limit <= 0 {
		return nil, nil
	}

	args := []interface{}{forward.RequestID}
	fromCondition := reverseLocationSQL(
		"route_from", "route_from_region", "route_from_full",
		forward.RouteTo, forward.RouteToRegion, forward.RouteToFull,
		&args,
	)
	toCondition := reverseLocationSQL(
		"route_to", "route_to_region", "route_to_full",
		forward.RouteFrom, forward.RouteFromRegion, forward.RouteFromFull,
		&args,
	)
	if fromCondition == "1=0" || toCondition == "1=0" {
		return nil, nil
	}

	extra := []string{}
	if filter != nil {
		if filter.MinWeight > 0 {
			args = append(args, filter.MinWeight)
			extra = append(extra, fmt.Sprintf("weight_t >= $%d", len(args)))
		}
		if filter.MaxWeight > 0 {
			args = append(args, filter.MaxWeight)
			extra = append(extra, fmt.Sprintf("weight_t IS NOT NULL AND weight_t <= $%d", len(args)))
		}
		if filter.MinVolume > 0 {
			args = append(args, filter.MinVolume)
			extra = append(extra, fmt.Sprintf("volume_m3 >= $%d", len(args)))
		}
		if filter.MaxVolume > 0 {
			args = append(args, filter.MaxVolume)
			extra = append(extra, fmt.Sprintf("volume_m3 IS NOT NULL AND volume_m3 <= $%d", len(args)))
		}
		for _, item := range []struct {
			min, max float64
			col      string
		}{
			{filter.MinLength, filter.MaxLength, "length_m"},
			{filter.MinWidth, filter.MaxWidth, "width_m"},
			{filter.MinHeight, filter.MaxHeight, "height_m"},
		} {
			if item.min > 0 {
				args = append(args, item.min)
				extra = append(extra, fmt.Sprintf("%s >= $%d", item.col, len(args)))
			}
			if item.max > 0 {
				args = append(args, item.max)
				extra = append(extra, fmt.Sprintf("%s IS NOT NULL AND %s <= $%d", item.col, item.col, len(args)))
			}
		}
		if filter.MinPricePerKm > 0 {
			args = append(args, filter.MinPricePerKm)
			extra = append(extra, fmt.Sprintf("price_per_km_uah >= $%d", len(args)))
		}
		if len(filter.TransportTypes) > 0 {
			args = append(args, filter.TransportTypes)
			extra = append(extra, fmt.Sprintf("transport_types && $%d::text[]", len(args)))
		}
	}

	limitPosition := len(args) + 1
	args = append(args, limit)

	where := []string{
		"request_id <> $1",
		"created_at >= NOW() - INTERVAL '48 HOURS'",
		fromCondition,
		toCondition,
	}
	where = append(where, extra...)

	query := fmt.Sprintf(`
        SELECT request_id,
               route_from,
               route_to,
               COALESCE(route_from_full, ''),
               COALESCE(route_to_full, ''),
               COALESCE(route_from_region, ''),
               COALESCE(route_to_region, ''),
               COALESCE(cargo_type, ''),
               COALESCE(distance_km, 0),
               COALESCE(weight_t, 0),
               COALESCE(volume_m3, 0),
               COALESCE(price_uah, 0),
               COALESCE(price_per_km_uah, 0),
               COALESCE(tags, '{}'),
               COALESCE(length_m, 0),
               COALESCE(width_m, 0),
               COALESCE(height_m, 0),
               COALESCE(transport_types, '{}'),
               COALESCE(published_relative, ''),
               COALESCE(published_at, '')
        FROM cargo_history
        WHERE %s
        ORDER BY created_at DESC
        LIMIT $%d
    `, strings.Join(where, " AND "), limitPosition)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*CargoPayload, 0, limit)
	for rows.Next() {
		var c CargoPayload
		if err := rows.Scan(
			&c.RequestID,
			&c.RouteFrom,
			&c.RouteTo,
			&c.RouteFromFull,
			&c.RouteToFull,
			&c.RouteFromRegion,
			&c.RouteToRegion,
			&c.CargoType,
			&c.DistanceKm,
			&c.WeightT,
			&c.VolumeM3,
			&c.PriceUAH,
			&c.PricePerKmUAH,
			&c.Tags,
			&c.LengthM,
			&c.WidthM,
			&c.HeightM,
			&c.TransportTypes,
			&c.PublishedRelative,
			&c.PublishedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func reverseLocationSQL(cityColumn, regionColumn, fullColumn, targetCity, targetRegion, targetFull string, args *[]interface{}) string {
	alternatives := make([]string, 0, 3)

	city := normalizeFilterValue(targetCity)
	if city != "" {
		*args = append(*args, city)
		alternatives = append(alternatives, fmt.Sprintf("LOWER(COALESCE(%s, '')) = $%d", cityColumn, len(*args)))
	}

	region := normalizeFilterValue(targetRegion)
	if region != "" {
		*args = append(*args, region)
		alternatives = append(alternatives, fmt.Sprintf("LOWER(COALESCE(%s, '')) = $%d", regionColumn, len(*args)))
	} else if targetFull != "" {
		// Full title is a defensive fallback for older cargo rows where the
		// normalized region column may be empty.
		*args = append(*args, "%"+normalizeFilterValue(targetFull)+"%")
		alternatives = append(alternatives, fmt.Sprintf("LOWER(COALESCE(%s, '')) LIKE $%d", fullColumn, len(*args)))
	}

	if len(alternatives) == 0 {
		return "1=0"
	}
	return "(" + strings.Join(alternatives, " OR ") + ")"
}

func formatReturnCandidates(candidates []*CargoPayload) string {
	if len(candidates) == 0 {
		return ""
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\n\n🔄 <b>Можливі зворотні вантажі (48г): %d</b>\n", len(candidates))
	for i, c := range candidates {
		fmt.Fprintf(&b, "\n<b>%d.</b> %s ➔ %s <code>%d КМ</code>\n", i+1, escapeTelegramHTML(strings.ToUpper(c.RouteFrom)), escapeTelegramHTML(strings.ToUpper(c.RouteTo)), c.DistanceKm)
		if c.PriceUAH > 0 && c.PricePerKmUAH > 0 {
			fmt.Fprintf(&b, "💰 <b>%s ГРН</b> <code>(%.2f ГРН/КМ)</code>\n", formatUAH(c.PriceUAH), c.PricePerKmUAH)
		} else if c.PriceUAH > 0 {
			fmt.Fprintf(&b, "💰 <b>%s ГРН</b>\n", formatUAH(c.PriceUAH))
		} else {
			b.WriteString("💰 <b>СТАВКА НЕ ВКАЗАНА</b>\n")
		}
		b.WriteString(formatTags(c.Tags))
		b.WriteString("\n<blockquote>\n")
		fmt.Fprintf(&b, "📦 <i>Вантаж:</i> %s\n", escapeTelegramHTML(c.CargoType))
		fmt.Fprintf(&b, "⚖️ <i>Вага / Об'єм:</i> %.1f т · %.1f м³\n", c.WeightT, c.VolumeM3)
		dimensions := []string{}
		if c.LengthM > 0 {
			dimensions = append(dimensions, fmt.Sprintf("дов %.2f м", c.LengthM))
		}
		if c.WidthM > 0 {
			dimensions = append(dimensions, fmt.Sprintf("шир %.2f м", c.WidthM))
		}
		if c.HeightM > 0 {
			dimensions = append(dimensions, fmt.Sprintf("вис %.2f м", c.HeightM))
		}
		if len(dimensions) == 0 {
			b.WriteString("📐 <i>Габарити:</i> не вказані\n")
		} else {
			fmt.Fprintf(&b, "📐 <i>Габарити:</i> %s\n", escapeTelegramHTML(strings.Join(dimensions, " · ")))
		}
		fmt.Fprintf(&b, "🚛 <i>Тип авто:</i> %s\n", escapeTelegramHTML(strings.Join(c.TransportTypes, ", ")))
		fmt.Fprintf(&b, "⏱ <i>Опубліковано:</i> %s", escapeTelegramHTML(c.PublishedRelative))
		if c.PublishedAt != "" {
			fmt.Fprintf(&b, " (%s)", escapeTelegramHTML(formatPublishedTime(c.PublishedAt)))
		}
		b.WriteString("\n</blockquote>\n")
	}
	return b.String()
}

func escapeTelegramHTML(value string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
	)
	return replacer.Replace(value)
}

func recordNewRoundTripPairs(ctx context.Context, db *pgxpool.Pool, chatID int64, forwardID string, candidates []*CargoPayload) ([]*CargoPayload, error) {
	if db == nil || forwardID == "" || len(candidates) == 0 {
		return nil, nil
	}

	returnIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate != nil && candidate.RequestID != "" {
			returnIDs = append(returnIDs, candidate.RequestID)
		}
	}
	if len(returnIDs) == 0 {
		return nil, nil
	}

	rows, err := db.Query(ctx, `
		INSERT INTO round_trip_pairs (chat_id, forward_request_id, return_request_id)
		SELECT $1, $2, unnest($3::varchar[])
		ON CONFLICT (chat_id, forward_request_id, return_request_id) DO NOTHING
		RETURNING return_request_id
	`, chatID, forwardID, returnIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	newIDs := make(map[string]struct{}, len(returnIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		newIDs[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	newCandidates := make([]*CargoPayload, 0, len(newIDs))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if _, ok := newIDs[candidate.RequestID]; ok {
			newCandidates = append(newCandidates, candidate)
		}
	}
	return newCandidates, nil
}

func formatRoundTripAlert(forward *CargoPayload, returns []*CargoPayload) string {
	var b strings.Builder
	b.WriteString("🚛 <b>Знайдено комплект туди + назад</b>\n\n")
	b.WriteString(formatAlert(forward, nil))
	b.WriteString(formatReturnCandidates(returns))
	return b.String()
}
