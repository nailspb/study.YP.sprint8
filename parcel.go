package main

import (
	"database/sql"
	"errors"
	"log"
	_ "modernc.org/sqlite"
	"os"
)

const (
	defaultDbFilePath = "./tracker.db"
)

const (
	ErrorDatabaseFileNotFound = "database file not found"
	ErrorOpenDatabase         = "error opening database"
	ErrorVerifyConnection     = "error verifying connection"
	BadParcelId               = "invalid parcel number"
	BadParcelIdOrStatus       = "invalid parcel number or its status is different from 'registered'"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(dbPath string) ParcelStore {
	if dbPath == "" {
		dbPath = defaultDbFilePath
	}
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		log.Fatalf("%s: %s", ErrorDatabaseFileNotFound, dbPath)
	}
	db, err := sql.Open("sqlite", defaultDbFilePath)
	if err != nil {
		log.Fatalf("%s", ErrorOpenDatabase)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("%s", ErrorVerifyConnection)
	}
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec("INSERT INTO parcel(`client`, `status`, `address`, `created_at`) VALUES (?, ?, ?, ?)", p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		return 0, err
	}
	// верните идентификатор последней добавленной записи
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(id), nil

}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("SELECT `number`, `client`, `status`, `address`, `created_at` FROM parcel WHERE number=$1", number)
	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return Parcel{}, err
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	rows, err := s.db.Query("SELECT `number`, `client`, `status`, `address`, `created_at` FROM parcel WHERE client=$1", client)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	// заполните срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		p := Parcel{}
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	res, err := s.db.Exec("UPDATE parcel SET `status`=$1 WHERE `number`=$2", status, number)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New(BadParcelId)
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	res, err := s.db.Exec("UPDATE parcel SET `address`=$1 WHERE `number`=$2 AND status=$3", address, number, ParcelStatusRegistered)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New(BadParcelIdOrStatus)
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	res, err := s.db.Exec("DELETE FROM parcel WHERE `number`=$1 AND status=$2", number, ParcelStatusRegistered)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New(BadParcelIdOrStatus)
	}
	return nil
}

func (s ParcelStore) Close() error {
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}
