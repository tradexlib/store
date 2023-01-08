package store

import "reflect"

func contains(s interface{}, e interface{}) bool {
	slice := convertSliceToInterface(s)

	for _, a := range slice {
		if a == e {
			return true
		}
	}

	return false
}

func convertSliceToInterface(s interface{}) (slice []interface{}) {
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Slice {
		return nil
	}

	length := v.Len()
	slice = make([]interface{}, length)
	for i := 0; i < length; i++ {
		slice[i] = v.Index(i).Interface()
	}

	return slice
}
