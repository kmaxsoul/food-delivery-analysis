-- schema_baghdad.sql
DROP DATABASE IF EXISTS food_delivery;
CREATE DATABASE food_delivery
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE food_delivery;


CREATE TABLE drivers (
  driver_id   INT PRIMARY KEY AUTO_INCREMENT,
  driver_name VARCHAR(100) NOT NULL,
  rating      DECIMAL(3,2) CHECK (rating BETWEEN 1 AND 5),
  start_date  DATE NOT NULL
);


CREATE TABLE customers (
  customer_id   INT PRIMARY KEY AUTO_INCREMENT,
  customer_name VARCHAR(100) NOT NULL,
  city          VARCHAR(100) NOT NULL,  
  signup_date   DATE NOT NULL
);


CREATE TABLE vendors (
  vendor_id    INT PRIMARY KEY AUTO_INCREMENT,
  vendor_name  VARCHAR(150) NOT NULL,
  cuisine      VARCHAR(80)  NOT NULL,    
  area         VARCHAR(100) NOT NULL,   
  rating       DECIMAL(3,2) CHECK (rating BETWEEN 1 AND 5),
  join_date    DATE NOT NULL
);

-- الطلبات
CREATE TABLE orders (
  order_id         BIGINT PRIMARY KEY AUTO_INCREMENT,
  customer_id      INT NOT NULL,
  driver_id        INT NOT NULL,
  vendor_id        INT NOT NULL,         
  food_category    VARCHAR(100) NOT NULL, 
  order_datetime   DATETIME NOT NULL,
  pickup_area      VARCHAR(100) NOT NULL,
  dropoff_area     VARCHAR(100) NOT NULL,
  distance_km      DECIMAL(5,2) NOT NULL,
  delivery_minutes INT NOT NULL,
  subtotal         DECIMAL(10,2) NOT NULL,
  delivery_fee     DECIMAL(10,2) NOT NULL,
  tip              DECIMAL(10,2) NOT NULL,
  driver_rating    DECIMAL(3,2),
  status           ENUM('delivered','canceled','returned') NOT NULL DEFAULT 'delivered',
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_orders_driver   FOREIGN KEY (driver_id)   REFERENCES drivers(driver_id),
  CONSTRAINT fk_orders_vendor   FOREIGN KEY (vendor_id)   REFERENCES vendors(vendor_id)
);


CREATE INDEX idx_orders_datetime ON orders(order_datetime);
CREATE INDEX idx_orders_driver   ON orders(driver_id);
CREATE INDEX idx_orders_vendor   ON orders(vendor_id);
CREATE INDEX idx_orders_droparea ON orders(dropoff_area);
