//  Copyright (c) 2018 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package tracing

import (
	"github.com/troubling/hummingbird/common/srv"
	"go.uber.org/zap"
)

// TraceLogger is an adapter from LowLevelLogger to jaeger-lib Logger interface.
type TraceLogger struct {
	logger *zap.SugaredLogger
}

// NewLogger creates a new Sugared Logger.
func NewTraceLogger(logger srv.LowLevelLogger) *TraceLogger {
	return &TraceLogger{logger: logger.Sugar()}
}

// Error logs a message at error priority
func (l *TraceLogger) Error(msg string) {
	l.logger.Error(msg)
}

// Infof logs a message at info priority
func (l *TraceLogger) Infof(msg string, args ...interface{}) {
	l.logger.Infof(msg, args...)
}
