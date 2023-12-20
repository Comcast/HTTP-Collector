package config

type Tracing struct {
	AppID string `json:"AppID"`
}

// Validate is a just a stub right now
// Not sure whether AppID should be 16 hex digits or any string
func (t *Tracing) Validate() error {
	return nil
}
