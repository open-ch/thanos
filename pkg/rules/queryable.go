// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/internal/cortex/querier/series"
	"github.com/thanos-io/thanos/pkg/clientconfig"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type promClientsQueryable struct {
	httpMethod string
	step       time.Duration

	logger            log.Logger
	promClients       []*promclient.Client
	queryClients      []*clientconfig.HTTPClient
	ignoredLabelNames []string

	duplicatedQuery prometheus.Counter
}
type promClientsQuerier struct {
	mint, maxt int64
	step       int64
	httpMethod string

	logger              log.Logger
	promClients         []*promclient.Client
	queryClients        []*clientconfig.HTTPClient
	restoreIgnoreLabels []string

	// We use a dummy counter here because the duplicated
	// addresses are already tracked by rule evaluation part.
	duplicatedQuery prometheus.Counter
}

// NewPromClientsQueryable creates a queryable that queries queriers from Prometheus clients.
func NewPromClientsQueryable(logger log.Logger, queryClients []*clientconfig.HTTPClient, promClients []*promclient.Client,
	httpMethod string, step time.Duration, ignoredLabelNames []string) *promClientsQueryable {
	return &promClientsQueryable{
		logger:            logger,
		queryClients:      queryClients,
		promClients:       promClients,
		duplicatedQuery:   promauto.With(nil).NewCounter(prometheus.CounterOpts{}),
		httpMethod:        httpMethod,
		step:              step,
		ignoredLabelNames: ignoredLabelNames,
	}
}

// Querier returns a new Querier for the given time range.
func (q *promClientsQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &promClientsQuerier{
		mint:                mint,
		maxt:                maxt,
		step:                int64(q.step / time.Second),
		httpMethod:          q.httpMethod,
		logger:              q.logger,
		queryClients:        q.queryClients,
		promClients:         q.promClients,
		restoreIgnoreLabels: q.ignoredLabelNames,
	}, nil
}

// Select implements storage.Querier interface.
func (q *promClientsQuerier) Select(ctx context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	query := storepb.PromMatchersToString(matchers...)

	// Enhanced logging for alert state restoration queries
	isAlertsForStateQuery := strings.Contains(query, "ALERTS_FOR_STATE")

	if isAlertsForStateQuery {
		level.Info(q.logger).Log(
			"msg", "executing alert state restoration query (verbose)",
			"query", query,
			"time_range", fmt.Sprintf("%d to %d", q.mint, q.maxt),
			"time_range_human", fmt.Sprintf("%s to %s",
				time.Unix(q.mint/1000, 0).Format(time.RFC3339),
				time.Unix(q.maxt/1000, 0).Format(time.RFC3339)),
			"step", q.step,
			"ignored_labels", strings.Join(q.restoreIgnoreLabels, ","),
			"matchers_count", len(matchers),
		)

		// Log each matcher in detail
		for i, matcher := range matchers {
			level.Info(q.logger).Log(
				"msg", "ALERTS_FOR_STATE query matcher detail",
				"matcher_index", i,
				"matcher_type", matcher.Type.String(),
				"matcher_name", matcher.Name,
				"matcher_value", matcher.Value,
			)
		}
	} else {
		level.Debug(q.logger).Log(
			"msg", "executing query via promClientsQuerier",
			"query", query,
			"time_range", fmt.Sprintf("%d to %d", q.mint, q.maxt),
		)
	}

	for _, i := range rand.Perm(len(q.queryClients)) {
		promClient := q.promClients[i]
		endpoints := RemoveDuplicateQueryEndpoints(q.logger, q.duplicatedQuery, q.queryClients[i].Endpoints())
		for _, i := range rand.Perm(len(endpoints)) {
			if isAlertsForStateQuery {
				level.Info(q.logger).Log(
					"msg", "trying alert state restoration query on endpoint",
					"endpoint", endpoints[i].String(),
					"query", query,
				)
			}

			m, warns, _, err := promClient.QueryRange(ctx, endpoints[i], query, q.mint, q.maxt, q.step, promclient.QueryOptions{
				Deduplicate: true,
				Method:      q.httpMethod,
			})

			if err != nil {
				if isAlertsForStateQuery {
					level.Error(q.logger).Log(
						"msg", "alert state restoration query failed",
						"err", err,
						"query", query,
						"endpoint", endpoints[i].String(),
					)
				} else {
					level.Error(q.logger).Log("err", err, "query", query)
				}
				continue
			}
			if len(warns) > 0 {
				level.Warn(q.logger).Log("warnings", strings.Join(warns, ", "), "query", query)
			}

			matrix := make([]*model.SampleStream, 0, m.Len())
			seriesIndex := 0
			for _, metric := range m {
				seriesIndex++

				// Log original labels before filtering - very verbose
				if isAlertsForStateQuery {
					level.Info(q.logger).Log(
						"msg", "processing ALERTS_FOR_STATE series (verbose)",
						"series_index", seriesIndex,
						"total_series", m.Len(),
						"labels", metric.Metric.String(),
						"values_count", len(metric.Values),
					)

					// Log each individual label
					for labelName, labelValue := range metric.Metric {
						level.Info(q.logger).Log(
							"msg", "ALERTS_FOR_STATE series label detail",
							"series_index", seriesIndex,
							"label_name", string(labelName),
							"label_value", string(labelValue),
						)
					}

					// Log all timestamp/value pairs if not too many
					if len(metric.Values) > 0 && len(metric.Values) <= 10 {
						for i, value := range metric.Values {
							level.Info(q.logger).Log(
								"msg", "ALERTS_FOR_STATE series all values",
								"series_index", seriesIndex,
								"value_index", i,
								"timestamp", value.Timestamp,
								"timestamp_human", time.Unix(int64(value.Timestamp)/1000, 0).Format(time.RFC3339),
								"value", value.Value,
							)
						}
					} else if len(metric.Values) > 0 {
						// Just log first, last and count for series with many values
						firstValue := metric.Values[0]
						lastValue := metric.Values[len(metric.Values)-1]
						level.Info(q.logger).Log(
							"msg", "ALERTS_FOR_STATE series value summary (too many to log all)",
							"series_index", seriesIndex,
							"first_timestamp", firstValue.Timestamp,
							"first_timestamp_human", time.Unix(int64(firstValue.Timestamp)/1000, 0).Format(time.RFC3339),
							"first_value", firstValue.Value,
							"last_timestamp", lastValue.Timestamp,
							"last_timestamp_human", time.Unix(int64(lastValue.Timestamp)/1000, 0).Format(time.RFC3339),
							"last_value", lastValue.Value,
							"total_values", len(metric.Values),
						)
					}
				}

				originalLabels := metric.Metric.Clone()

				// Log each label that will be removed
				for _, label := range q.restoreIgnoreLabels {
					if _, exists := metric.Metric[model.LabelName(label)]; exists && isAlertsForStateQuery {
						level.Info(q.logger).Log(
							"msg", "removing ignored label from ALERTS_FOR_STATE series",
							"series_index", seriesIndex,
							"removed_label", label,
							"removed_value", string(metric.Metric[model.LabelName(label)]),
						)
					}
					delete(metric.Metric, model.LabelName(label))
				}

				// Log labels after filtering - very verbose
				if isAlertsForStateQuery {
					level.Info(q.logger).Log(
						"msg", "ALERTS_FOR_STATE series after label filtering (verbose)",
						"series_index", seriesIndex,
						"original_labels", originalLabels.String(),
						"filtered_labels", metric.Metric.String(),
						"removed_labels", strings.Join(q.restoreIgnoreLabels, ","),
						"values_count", len(metric.Values),
					)

					// Log each remaining label after filtering
					for labelName, labelValue := range metric.Metric {
						level.Info(q.logger).Log(
							"msg", "ALERTS_FOR_STATE series remaining label after filtering",
							"series_index", seriesIndex,
							"label_name", string(labelName),
							"label_value", string(labelValue),
						)
					}
				}

				matrix = append(matrix, &model.SampleStream{
					Metric: metric.Metric,
					Values: metric.Values,
				})
			}

			if isAlertsForStateQuery {
				level.Info(q.logger).Log(
					"msg", "alert state restoration query completed (verbose)",
					"query", query,
					"series_found", len(matrix),
					"endpoint", endpoints[i].String(),
				)

				// Log summary of all series found
				for i, stream := range matrix {
					level.Info(q.logger).Log(
						"msg", "final ALERTS_FOR_STATE series in result set",
						"result_index", i,
						"labels", stream.Metric.String(),
						"values_count", len(stream.Values),
					)
				}
			}

			return series.MatrixToSeriesSet(matrix)
		}
	}

	if isAlertsForStateQuery {
		level.Warn(q.logger).Log(
			"msg", "alert state restoration query found no results",
			"query", query,
		)
	}

	return storage.NoopSeriesSet()
}

// LabelValues implements storage.LabelQuerier interface.
func (q *promClientsQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

// LabelNames implements storage.LabelQuerier interface.
func (q *promClientsQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

// Close implements storage.LabelQuerier interface.
func (q *promClientsQuerier) Close() error {
	return nil
}

// RemoveDuplicateQueryEndpoints removes duplicate endpoints from the list of urls.
func RemoveDuplicateQueryEndpoints(logger log.Logger, duplicatedQueriers prometheus.Counter, urls []*url.URL) []*url.URL {
	set := make(map[string]struct{})
	deduplicated := make([]*url.URL, 0, len(urls))
	for _, u := range urls {
		if _, ok := set[u.String()]; ok {
			level.Warn(logger).Log("msg", "duplicate query address is provided", "addr", u.String())
			duplicatedQueriers.Inc()
			continue
		}
		deduplicated = append(deduplicated, u)
		set[u.String()] = struct{}{}
	}
	return deduplicated
}
