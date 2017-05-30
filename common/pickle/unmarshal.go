package pickle

import (
	"errors"
	"reflect"
)

func unpack(src reflect.Value, dst reflect.Value) error {
	for src.Kind() == reflect.Ptr || src.Kind() == reflect.Interface {
		if src.IsNil() {
			dst.Set(dst)
			return nil
		}
		src = src.Elem()
	}
	for dst.Kind() == reflect.Ptr || dst.Kind() == reflect.Interface {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	if src.Type().ConvertibleTo(dst.Type()) {
		dst.Set(src.Convert(dst.Type()))
		return nil
	}
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(src.Interface() != reflect.Zero(src.Type()).Interface())
		return nil
	case reflect.Slice:
		if k := src.Kind(); k != reflect.Slice && k != reflect.Array {
			return errors.New("Unable to assign slice from non-slice")
		}
		dst.Set(reflect.MakeSlice(dst.Type(), src.Len(), src.Len()))
		for i := 0; i < src.Len(); i++ {
			if err := unpack(src.Index(i), dst.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if src.Kind() != reflect.Map {
			return errors.New("Unable to assign map from non-map")
		}
		dst.Set(reflect.MakeMap(dst.Type()))
		nk := reflect.New(dst.Type().Key())
		nv := reflect.New(dst.Type().Elem())
		for _, k := range src.MapKeys() {
			if err := unpack(k, nk); err != nil {
				return err
			}
			if err := unpack(src.MapIndex(k), nv); err != nil {
				return err
			}
			dst.SetMapIndex(nk.Elem(), nv.Elem())
		}
	case reflect.Struct:
		if src.Kind() != reflect.Map {
			return errors.New("Unable to assign struct from non-map")
		}
		for _, k := range src.MapKeys() {
			for k.Kind() == reflect.Ptr || k.Kind() == reflect.Interface {
				k = k.Elem()
			}
			for i := 0; i < dst.NumField(); i++ {
				if f := dst.Type().Field(i); f.Name == k.String() || f.Tag.Get("pickle") == k.String() {
					if err := unpack(src.MapIndex(k), dst.Field(i)); err != nil {
						return err
					}
				}
			}
		}
	default:
		return errors.New("Assign to unknown type")
	}
	return nil
}

// Unmarshal parses the pickled data and stores the result in the value pointed to by v.
// Unmarshal is considerably less performant than PickleLoads, so think twice before using it in performance-sensitive code.
func Unmarshal(data []byte, v interface{}) error {
	src, err := PickleLoads(data)
	if err != nil {
		return err
	}
	return unpack(reflect.ValueOf(src), reflect.ValueOf(v))
}
