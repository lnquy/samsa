package condition

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

var log = logrus.WithFields(logrus.Fields{"package": "condition"})

type DataType int

var (
	DataTypePlainText DataType = 0
	DataTypeJSON      DataType = 1
)

// TODO: Cond is dead simple and has many limitations atm
type Cond struct {
	DataType DataType
	Field    string
	Value    string
}

// condition format: field.nested_field=expected_value
func Parse(cond, dataType string) (Cond, error) {
	typ := DataTypePlainText
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "json":
		typ = DataTypeJSON
		parts := strings.Split(cond, "=")
		if len(parts) != 2 {
			return Cond{}, fmt.Errorf("invalid condition, expected exactly 1 equal sign (=) in input, got=%q", cond)
		}
		return Cond{
			DataType: typ,
			Field:    strings.TrimSpace(parts[0]),
			Value:    strings.TrimSpace(parts[1]),
		}, nil
	default:
		typ = DataTypePlainText // [?] Should default plaintext or must be explicitly defined
		return Cond{
			DataType: typ,
			Field:    "",
			Value:    strings.TrimSpace(cond),
		}, nil
	}
}

func (c Cond) Match(data []byte) bool {
	switch c.DataType {
	case DataTypePlainText:
		return fmt.Sprintf("%s", data) == c.Value // Just assume everything is string atm
	case DataTypeJSON:
		type raw map[string]interface{}
		var levelData raw
		if err := json.Unmarshal(data, &levelData); err != nil {
			// log.Debugf("failed to decode data to JSON: %s, data=%s", err, data)
			return false
		}

		fieldLevels := strings.Split(c.Field, ".")
		for lvl, field := range fieldLevels {
			v, ok := levelData[field]
			if !ok {
				// log.Debugf("field not found: field=%s, levelData=%s", field, levelData)
				return false
			}

			// If the condition has nested levels.
			// => Cast v to map[string]interface{} then continue looking for final field
			if lvl < len(fieldLevels)-1 {
				castedMap, ok := v.(map[string]interface{})
				if !ok { // If field is not a map
					// log.Debugf("field value is not a map: field=%s, levelData=%s, value=%s", field, levelData, v)
					return false
				}
				levelData = castedMap
				continue // Continue to go deeper to find the final field condition and its value
			}

			// log.Debugf("found: value=%s, expected=%s", v, c.Value)
			// Finally found the field condition and its value
			return fmt.Sprint(v) == c.Value // Just assume everything is string atm
		}

	}

	return false
}
