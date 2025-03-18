package mdasio

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
)

var ErrNoFullWrite = errors.New("No full write")

type Unit struct {
	Id   string
	Name string
	Type string
}

type Point struct {
	Lat float32
	Lon float32
}

type Geometry string

type GridHeader struct {
	N       int
	M       int
	MinLat  float32 // -90 : 90
	MaxLat  float32
	MinLon  float32 // 0 : 360
	MaxLon  float32
	StepLat float32
	StepLon float32
}

type Grid struct {
	GridHeader
	Data [][]float32
}

type MdasIO struct {
	r   io.Reader
	w   io.Writer
	buf []byte
}

func NewMdasIO(r io.Reader, w io.Writer) *MdasIO {
	return &MdasIO{
		r:   r,
		w:   w,
		buf: make([]byte, 8),
	}
}

func (m *MdasIO) ReadBool() (bool, error) {
	if _, err := io.ReadFull(m.r, m.buf[:1]); err != nil {
		return false, err
	}
	return m.buf[0] != 0, nil
}

func (m *MdasIO) ReadInt16() (int16, error) {
	if _, err := io.ReadFull(m.r, m.buf[:2]); err != nil {
		return 0, err
	}
	return int16(binary.LittleEndian.Uint16(m.buf[:2])), nil
}

func (m *MdasIO) ReadInt() (int, error) {
	if _, err := io.ReadFull(m.r, m.buf[:4]); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(m.buf[:4])), nil
}

func (m *MdasIO) ReadInt64() (int64, error) {
	if _, err := io.ReadFull(m.r, m.buf[:8]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(m.buf[:8])), nil
}

func (m *MdasIO) ReadFloat() (float32, error) {
	if _, err := io.ReadFull(m.r, m.buf[:4]); err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(m.buf[:4])), nil
}

func (m *MdasIO) ReadString() (string, error) {
	strLen, err := m.ReadInt()
	if err != nil {
		return "", err
	}
	newBuf := make([]byte, strLen)
	if _, err := io.ReadFull(m.r, newBuf); err != nil {
		return "", err
	}
	return string(newBuf), nil
}

func (m *MdasIO) ReadDate() (time.Time, error) {
	if sec, err := m.ReadInt64(); err != nil {
		return time.Time{}, err
	} else {
		return time.Unix(sec, 0), err
	}
}

func (m *MdasIO) ReadDuration() (time.Duration, error) {
	if nsec, err := m.ReadInt64(); err != nil {
		return time.Duration(0), err
	} else {
		return time.Duration(nsec), err
	}
}

func (m *MdasIO) ReadPoint() (Point, error) {
	var point Point
	var err error

	if point.Lat, err = m.ReadFloat(); err != nil {
		return point, err
	}
	if point.Lon, err = m.ReadFloat(); err != nil {
		return point, err
	}
	return point, nil
}

func (m *MdasIO) ReadGeometry() (Geometry, error) {
	s, err := m.ReadString()
	return Geometry(s), err
}

func (m *MdasIO) ReadGrid() (Grid, error) {
	var grid Grid
	var err error

	if grid.N, err = m.ReadInt(); err != nil {
		return grid, err
	} else if grid.M, err = m.ReadInt(); err != nil {
		return grid, err
	} else if grid.MinLat, err = m.ReadFloat(); err != nil {
		return grid, err
	} else if grid.MaxLat, err = m.ReadFloat(); err != nil {
		return grid, err
	} else if grid.MinLon, err = m.ReadFloat(); err != nil {
		return grid, err
	} else if grid.MaxLon, err = m.ReadFloat(); err != nil {
		return grid, err
	} else if grid.StepLat, err = m.ReadFloat(); err != nil {
		return grid, err
	} else if grid.StepLon, err = m.ReadFloat(); err != nil {
		return grid, err
	}

	rowBuf := make([]byte, 4*grid.M)
	grid.Data = make([][]float32, grid.N)
	for i := 0; i < grid.N; i++ {
		if _, err := io.ReadFull(m.r, rowBuf); err != nil {
			return grid, err
		}
		grid.Data[i] = make([]float32, grid.M)
		for j := 0; j < grid.M; j++ {
			grid.Data[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(m.buf[4*j : 4*j+4]))
		}
	}

	return grid, nil
}

func (m *MdasIO) ReadUnit() (Unit, error) {
	var unit Unit
	var err error

	if unit.Id, err = m.ReadString(); err != nil {
		return unit, err
	}
	if unit.Name, err = m.ReadString(); err != nil {
		return unit, err
	}
	if unit.Type, err = m.ReadString(); err != nil {
		return unit, err
	}

	return unit, nil
}

func (m *MdasIO) WriteBool(v bool) error {
	if v {
		m.buf[0] = 1
	} else {
		m.buf[0] = 0
	}

	if n, err := m.w.Write(m.buf[:1]); err != nil {
		return err
	} else if n != 1 {
		return ErrNoFullWrite
	}
	return nil
}

func (m *MdasIO) WriteInt16(v int16) error {
	binary.LittleEndian.PutUint16(m.buf[:2], uint16(v))
	if n, err := m.w.Write(m.buf[:2]); err != nil {
		return err
	} else if n != 2 {
		return ErrNoFullWrite
	}
	return nil
}

func (m *MdasIO) WriteInt(v int) error {
	binary.LittleEndian.PutUint32(m.buf[:4], uint32(v))
	if n, err := m.w.Write(m.buf[:4]); err != nil {
		return err
	} else if n != 4 {
		return ErrNoFullWrite
	}
	return nil
}

func (m *MdasIO) WriteInt64(v int64) error {
	binary.LittleEndian.PutUint64(m.buf[:8], uint64(v))
	if n, err := m.w.Write(m.buf[:8]); err != nil {
		return err
	} else if n != 8 {
		return ErrNoFullWrite
	}
	return nil
}

func (m *MdasIO) WriteFloat(v float32) error {
	binary.LittleEndian.PutUint32(m.buf[:4], math.Float32bits(v))
	if n, err := m.w.Write(m.buf[:4]); err != nil {
		return err
	} else if n != 4 {
		return ErrNoFullWrite
	}
	return nil
}

func (m *MdasIO) writeAll(b []byte) error {
	for len(b) > 0 {
		n, err := m.w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func (m *MdasIO) WriteString(v string) error {
	if err := m.WriteInt(len(v)); err != nil {
		return err
	}
	return m.writeAll([]byte(v))
}

func (m *MdasIO) WriteDate(v time.Time) error {
	return m.WriteInt64(v.Unix())
}

func (m *MdasIO) WriteDuration(v time.Duration) error {
	return m.WriteInt64(int64(v))
}

func (m *MdasIO) WritePoint(v Point) error {
	if err := m.WriteFloat(v.Lat); err != nil {
		return err
	}
	return m.WriteFloat(v.Lon)
}

func (m *MdasIO) WriteGeometry(v Geometry) error {
	return m.WriteString(string(v))
}

func (m *MdasIO) WriteGrid(v Grid) error {
	if err := m.WriteInt(v.N); err != nil {
		return err
	} else if err := m.WriteInt(v.M); err != nil {
		return err
	} else if err := m.WriteFloat(v.MinLat); err != nil {
		return err
	} else if err := m.WriteFloat(v.MaxLat); err != nil {
		return err
	} else if err := m.WriteFloat(v.MinLon); err != nil {
		return err
	} else if err := m.WriteFloat(v.MaxLon); err != nil {
		return err
	} else if err := m.WriteFloat(v.StepLat); err != nil {
		return err
	} else if err := m.WriteFloat(v.StepLon); err != nil {
		return err
	}

	rowBuf := make([]byte, 4*v.M)
	for i := 0; i < v.N; i++ {
		for j := 0; j < v.M; j++ {
			binary.LittleEndian.PutUint32(rowBuf[4*j:4*j+4], math.Float32bits(v.Data[i][j]))
		}
		if err := m.writeAll(rowBuf); err != nil {
			return err
		}
	}

	return nil
}
