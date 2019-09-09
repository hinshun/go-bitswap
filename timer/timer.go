package timer

import (
	"context"
	"time"
)

type timeKey struct{}

func WithTime(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, timeKey{}, t)
}

func Time(ctx context.Context) time.Time {
	return ctx.Value(timeKey{}).(time.Time)
}
