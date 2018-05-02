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
	"io"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

// Init creates a new instance of Jaeger tracer.
func Init(serviceName string, logger srv.LowLevelLogger, section conf.Section) (opentracing.Tracer, io.Closer, error) {
	cfg := config.Configuration{
		Disabled: section.GetBool("disabled", false),
		Sampler: &config.SamplerConfig{
			Type:  section.GetDefault("sampler_type", "const"),
			Param: section.GetFloat("sampler_param", 1),
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            section.GetBool("reporter_log_spans", false),
			BufferFlushInterval: 1 * time.Second,
			LocalAgentHostPort:  section.GetDefault("agent_host_port", ""),
		},
	}
	tracer, closer, err := cfg.New(
		serviceName,
		config.Logger(NewTraceLogger(logger)),
	)
	return tracer, closer, err
}
