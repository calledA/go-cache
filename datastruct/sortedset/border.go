package sortedset

import (
	"errors"
	"strconv"
)

/**
 * @Author: wanglei
 * @File: border
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/19 12:16
 */

const (
	negativeInf int8 = -1
	positiveInf int8 = 1
)

type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

func (b *ScoreBorder) greater(val float64) bool {
	if b.Inf == negativeInf {
		return false
	} else if b.Inf == positiveInf {
		return true
	}

	if b.Exclude {
		return b.Value > val
	}
	return b.Value >= val
}

func (b *ScoreBorder) less(val float64) bool {
	if b.Inf == negativeInf {
		return true
	} else if b.Inf == positiveInf {
		return false
	}

	if b.Exclude {
		return b.Value < val
	}
	return b.Value <= val
}

var positiveInfBorder = &ScoreBorder{
	Inf: positiveInf,
}

var negativeInfBorder = &ScoreBorder{
	Inf: negativeInf,
}

func ParseScoreBorder(s string) (*ScoreBorder, error) {
	if s == "inf" || s == "+inf" {
		return positiveInfBorder, nil
	}
	if s == "-inf" {
		return negativeInfBorder, nil
	}

	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}

	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}
