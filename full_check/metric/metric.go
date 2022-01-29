package metric

type Metric struct {
	DateTime           string                             `json:"datetime"`
	Timestamp          int64                              `json:"timestamp"`
	CompareTimes       int                                `json:"comparetimes"`
	Id                 string                             `json:"id"`
	JobId              string                             `json:"jobid"`
	TaskId             string                             `json:"taskid"`
	Db                 int32                              `json:"db"`
	DbKeys             int64                              `json:"dbkeys"`
	Process            int64                              `json:"process"`
	OneCompareFinished bool                               `json:"has_finished"`
	AllFinished        bool                               `json:"all_finished"`
	KeyScan            *CounterStat                       `json:"key_scan"`
	TotalConflict      int64                              `json:"total_conflict"`
	TotalKeyConflict   int64                              `json:"total_key_conflict"`
	TotalFieldConflict int64                              `json:"total_field_conflict"`
	KeyMetric          map[string]map[string]*CounterStat `json:"key_stat"`
	FieldMetric        map[string]map[string]*CounterStat `json:"field_stat"`
}

type MetricItem struct {
	Type     string       `json:"type"`
	Conflict string       `json:"conflict"`
	Stat     *CounterStat `json:"stat"`
}