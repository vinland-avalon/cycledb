// Package engine can be imported to initialize and register all available TSDB engines.
//
// Alternatively, you can import any individual subpackage underneath engine.
package engine // import "cycledb/pkg/tsdb/engine"

import (

	// Initialize and register tsm engine
	_ "cycledb/pkg/tsdb/engine/tsm"
)
