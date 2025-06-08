//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"review-task/internal/biz"
	"review-task/internal/conf"
	"review-task/internal/data"
	"review-task/internal/server"
	"review-task/internal/service"
	"review-task/internal/task"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Kafka, *conf.Elasticsearch, *conf.Data, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, task.ProviderSet, newApp))
}
