package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test the pointer helper functions
func TestPtrHelpers(t *testing.T) {
	t.Run("PtrBool", func(t *testing.T) {
		val := true
		ptr := PtrBool(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)

		val2 := false
		ptr2 := PtrBool(val2)
		assert.NotNil(t, ptr2)
		assert.Equal(t, val2, *ptr2)
	})

	t.Run("PtrInt", func(t *testing.T) {
		val := 42
		ptr := PtrInt(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)

		val2 := -123
		ptr2 := PtrInt(val2)
		assert.NotNil(t, ptr2)
		assert.Equal(t, val2, *ptr2)
	})

	t.Run("PtrInt32", func(t *testing.T) {
		val := int32(123)
		ptr := PtrInt32(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)
	})

	t.Run("PtrInt64", func(t *testing.T) {
		val := int64(9876543210)
		ptr := PtrInt64(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)
	})

	t.Run("PtrFloat32", func(t *testing.T) {
		val := float32(3.14)
		ptr := PtrFloat32(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)
	})

	t.Run("PtrFloat64", func(t *testing.T) {
		val := 2.718281828459045
		ptr := PtrFloat64(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)
	})

	t.Run("PtrString", func(t *testing.T) {
		val := "hello world"
		ptr := PtrString(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)

		empty := ""
		ptrEmpty := PtrString(empty)
		assert.NotNil(t, ptrEmpty)
		assert.Equal(t, empty, *ptrEmpty)
	})

	t.Run("PtrTime", func(t *testing.T) {
		val := time.Now()
		ptr := PtrTime(val)
		assert.NotNil(t, ptr)
		assert.Equal(t, val, *ptr)
	})
}

// Test NullableBool
func TestNullableBool(t *testing.T) {
	t.Run("NewNullableBool", func(t *testing.T) {
		val := true
		nb := NewNullableBool(&val)
		assert.True(t, nb.IsSet())
		assert.Equal(t, &val, nb.Get())

		// When passing nil, it's still considered "set" (set to null)
		nb2 := NewNullableBool(nil)
		assert.True(t, nb2.IsSet()) // This is the actual behavior
		assert.Nil(t, nb2.Get())
	})

	t.Run("Set and Get", func(t *testing.T) {
		var nb NullableBool
		assert.False(t, nb.IsSet())
		assert.Nil(t, nb.Get())

		val := false
		nb.Set(&val)
		assert.True(t, nb.IsSet())
		assert.Equal(t, &val, nb.Get())
	})

	t.Run("Unset", func(t *testing.T) {
		val := true
		nb := NewNullableBool(&val)
		assert.True(t, nb.IsSet())

		nb.Unset()
		assert.False(t, nb.IsSet())
		assert.Nil(t, nb.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		// Test with value
		val := true
		nb := NewNullableBool(&val)
		data, err := nb.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, "true", string(data))

		// Test without value (null)
		nb2 := NewNullableBool(nil)
		data2, err2 := nb2.MarshalJSON()
		assert.NoError(t, err2)
		assert.Equal(t, "null", string(data2))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var nb NullableBool

		// Test unmarshaling true
		err := nb.UnmarshalJSON([]byte("true"))
		assert.NoError(t, err)
		assert.True(t, nb.IsSet())
		assert.True(t, *nb.Get())

		// Test unmarshaling false
		err = nb.UnmarshalJSON([]byte("false"))
		assert.NoError(t, err)
		assert.True(t, nb.IsSet())
		assert.False(t, *nb.Get())

		// Test unmarshaling null - this sets isSet to true but value to nil
		err = nb.UnmarshalJSON([]byte("null"))
		assert.NoError(t, err)
		assert.True(t, nb.IsSet()) // Still considered "set"
		assert.Nil(t, nb.Get())
	})
}

// Test NullableInt
func TestNullableInt(t *testing.T) {
	t.Run("NewNullableInt", func(t *testing.T) {
		val := 42
		ni := NewNullableInt(&val)
		assert.True(t, ni.IsSet())
		assert.Equal(t, &val, ni.Get())
	})

	t.Run("Set and Get", func(t *testing.T) {
		var ni NullableInt
		val := 123
		ni.Set(&val)
		assert.True(t, ni.IsSet())
		assert.Equal(t, &val, ni.Get())
	})

	t.Run("Unset", func(t *testing.T) {
		val := 42
		ni := NewNullableInt(&val)
		ni.Unset()
		assert.False(t, ni.IsSet())
		assert.Nil(t, ni.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := 42
		ni := NewNullableInt(&val)
		data, err := ni.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, "42", string(data))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var ni NullableInt
		err := ni.UnmarshalJSON([]byte("123"))
		assert.NoError(t, err)
		assert.True(t, ni.IsSet())
		assert.Equal(t, 123, *ni.Get())
	})
}

// Test NullableInt32
func TestNullableInt32(t *testing.T) {
	t.Run("NewNullableInt32", func(t *testing.T) {
		val := int32(42)
		ni := NewNullableInt32(&val)
		assert.True(t, ni.IsSet())
		assert.Equal(t, &val, ni.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := int32(42)
		ni := NewNullableInt32(&val)
		data, err := ni.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, "42", string(data))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var ni NullableInt32
		err := ni.UnmarshalJSON([]byte("123"))
		assert.NoError(t, err)
		assert.True(t, ni.IsSet())
		assert.Equal(t, int32(123), *ni.Get())
	})
}

// Test NullableInt64
func TestNullableInt64(t *testing.T) {
	t.Run("NewNullableInt64", func(t *testing.T) {
		val := int64(9876543210)
		ni := NewNullableInt64(&val)
		assert.True(t, ni.IsSet())
		assert.Equal(t, &val, ni.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := int64(9876543210)
		ni := NewNullableInt64(&val)
		data, err := ni.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, "9876543210", string(data))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var ni NullableInt64
		err := ni.UnmarshalJSON([]byte("9876543210"))
		assert.NoError(t, err)
		assert.True(t, ni.IsSet())
		assert.Equal(t, int64(9876543210), *ni.Get())
	})
}

// Test NullableFloat32
func TestNullableFloat32(t *testing.T) {
	t.Run("NewNullableFloat32", func(t *testing.T) {
		val := float32(3.14)
		nf := NewNullableFloat32(&val)
		assert.True(t, nf.IsSet())
		assert.Equal(t, &val, nf.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := float32(3.14)
		nf := NewNullableFloat32(&val)
		data, err := nf.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, "3.14", string(data))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var nf NullableFloat32
		err := nf.UnmarshalJSON([]byte("3.14"))
		assert.NoError(t, err)
		assert.True(t, nf.IsSet())
		assert.InDelta(t, 3.14, *nf.Get(), 0.001)
	})
}

// Test NullableFloat64
func TestNullableFloat64(t *testing.T) {
	t.Run("NewNullableFloat64", func(t *testing.T) {
		val := 2.718281828459045
		nf := NewNullableFloat64(&val)
		assert.True(t, nf.IsSet())
		assert.Equal(t, &val, nf.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := 2.718281828459045
		nf := NewNullableFloat64(&val)
		data, err := nf.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, "2.718281828459045", string(data))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var nf NullableFloat64
		err := nf.UnmarshalJSON([]byte("2.718281828459045"))
		assert.NoError(t, err)
		assert.True(t, nf.IsSet())
		assert.InDelta(t, 2.718281828459045, *nf.Get(), 0.000000001)
	})
}

// Test NullableString
func TestNullableString(t *testing.T) {
	t.Run("NewNullableString", func(t *testing.T) {
		val := "hello world"
		ns := NewNullableString(&val)
		assert.True(t, ns.IsSet())
		assert.Equal(t, &val, ns.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := "hello world"
		ns := NewNullableString(&val)
		data, err := ns.MarshalJSON()
		assert.NoError(t, err)
		assert.Equal(t, `"hello world"`, string(data))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var ns NullableString
		err := ns.UnmarshalJSON([]byte(`"hello world"`))
		assert.NoError(t, err)
		assert.True(t, ns.IsSet())
		assert.Equal(t, "hello world", *ns.Get())
	})
}

// Test NullableTime
func TestNullableTime(t *testing.T) {
	t.Run("NewNullableTime", func(t *testing.T) {
		val := time.Now()
		nt := NewNullableTime(&val)
		assert.True(t, nt.IsSet())
		assert.Equal(t, &val, nt.Get())
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		val := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
		nt := NewNullableTime(&val)
		data, err := nt.MarshalJSON()
		assert.NoError(t, err)
		assert.Contains(t, string(data), "2023-01-01T12:00:00")
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		var nt NullableTime
		timeStr := `"2023-01-01T12:00:00Z"`
		err := nt.UnmarshalJSON([]byte(timeStr))
		assert.NoError(t, err)
		assert.True(t, nt.IsSet())

		expected := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
		assert.True(t, expected.Equal(*nt.Get()))
	})

	t.Run("IsNil", func(t *testing.T) {
		// Test with nil NullableTime pointer
		var nt *NullableTime
		assert.True(t, IsNil(nt))

		// Test with actual NullableTime instance (not nil)
		val := time.Now()
		nt2 := NewNullableTime(&val)
		assert.False(t, IsNil(nt2))

		// Test with NullableTime that has nil value (but struct itself is not nil)
		nt3 := NewNullableTime(nil)
		assert.False(t, IsNil(nt3)) // The struct is not nil, just its value is nil
	})
}

// Test utility functions
func TestUtilityFunctions(t *testing.T) {
	t.Run("newStrictDecoder", func(t *testing.T) {
		data := []byte(`{"test": "value"}`)
		decoder := newStrictDecoder(data)
		assert.NotNil(t, decoder)

		var result map[string]interface{}
		err := decoder.Decode(&result)
		assert.NoError(t, err)
		assert.Equal(t, "value", result["test"])
	})

	t.Run("reportError", func(t *testing.T) {
		// This function just formats error messages
		// Testing it by checking if it returns formatted error
		err := reportError("TestFunction error: %s", "test error message")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TestFunction")
		assert.Contains(t, err.Error(), "test error message")
	})
}
