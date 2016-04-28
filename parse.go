// Copyright 2016 Paul Stuart. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file.

// This file provides the Parse function, which will take
// a bufio Reader and apply "grok" style pattern matching
// and send the matched data to influxdb

package grokflux

import (
	"bufio"
	"fmt"
	"strconv"
	"time"

	"github.com/vjeantet/grok"
)

// Translate controls how the grok results are converted to a data point
type Translate struct {
	Pattern  string   // Pattern to apply when parsing data
	Key      string   // Lookup the value of Key, if found use it as point name, otherwise use Key itself
	TSLayout string   // Timestamp Layout for parsing
	TSField  string   // Field to be parsed for timestamp
	TFields  []string // List of tag fields to use
	VFields  []string // List of fields to use for values
}

// Filter defines a secondary groking of a result set
type Filter struct {
	Pattern string            // the grok pattern name
	Key     string            // the
	Good    []string          // list of keys of captured fields to use as values
	Tags    []string          // list of keys of captured fields to use as tags
	Valid   map[string]string // an optional map of values where
}

var (
	// Debug shows match results if enabled
	Debug bool

	// Directory specifies where to look for additional grok patterns
	Directory = "patterns"

	// ErrNoMatch is returned pattern did not match against input
	ErrNoMatch = fmt.Errorf("No match")
)

// numerical returns the parsed data type in its native form
func numerical(s string) interface{} {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if i, err := strconv.ParseInt(s, 0, 64); err == nil {
		return i
	}
	return s
}

// AddPoint will convert the values of m into an influxdb Point and queue it to send
func AddPoint(m, tags map[string]string, t Translate, send Sender) (err error) {
	found, ok := m[t.Key]
	if !ok {
		found = t.Key
	}
	for _, tag := range t.TFields {
		tags[tag] = m[tag]
	}
	f := make(map[string]interface{})
	for _, val := range t.VFields {
		f[val] = numerical(m[val])
	}

	var ts time.Time
	if len(t.TSLayout) > 0 && len(t.TSField) > 0 {
		if ts, err = time.Parse(t.TSLayout, m[t.TSField]); err != nil {
			return err
		}
	} else {
		ts = time.Now()
	}

	return send(found, tags, f, ts)
}

// refine will check to see if it is selected, then apply an optional
// secondary regex to the captured data
func refine(g *grok.Grok, data map[string]string, trans *Translate, f Filter) error {
	if f.Valid != nil {
		for k, v := range f.Valid {
			if data[k] != v {
				return ErrNoMatch
			}
		}
	}
	if len(f.Pattern) > 0 {
		matched, err := g.Parse(f.Pattern, data[f.Key])
		if err != nil {
			return err
		}
		// TODO: this can overwrite existing values. Bug or Feature?
		for k, v := range matched {
			// strip quotes
			if len(v) > 0 && v[0] == '"' {
				v = v[1 : len(v)-1]
			}
			data[k] = v
		}
	}
	trans.VFields = append(trans.VFields, f.Good...)
	trans.TFields = append(trans.TFields, f.Tags...)
	return nil
}

// Parse processes the data from reader line by line and
// sends results to influxdb as a data point
func Parse(
	reader *bufio.Reader, // data source
	s Sender, // where to write results
	trans Translate, // how to apply the patterns
	filters []Filter, // secondary filtering
	tags map[string]string, // additional tags to write
) error {

	g, _ := grok.New()

	if len(Directory) > 0 {
		if err := g.AddPatternsFromPath(Directory); err != nil {
			return err
		}
	}

	process := func(m map[string]string) error {
		if Debug {
			for k, v := range m {
				fmt.Println("Matched Key:", k, "Value:", v)
			}
		}
		t := trans
		for _, filter := range filters {
			if err := refine(g, m, &t, filter); err == nil {
				return err
			}
		}
		return AddPoint(m, tags, t, s)
	}

	return g.ParseStream(reader, trans.Pattern, process)
}
