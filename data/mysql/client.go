package mysql

import (
	"database/sql"
	"time"

	"globalmail/config"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func Open(cfg config.MySQLConfig) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	ConfigurePool(sqlDB, cfg)
	return db, nil
}

func ConfigurePool(db *sql.DB, cfg config.MySQLConfig) {
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	lifetime := cfg.ConnMaxLifetime.Duration
	if lifetime <= 0 {
		lifetime = time.Hour
	}
	db.SetConnMaxLifetime(lifetime)
}
