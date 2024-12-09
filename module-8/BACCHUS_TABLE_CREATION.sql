from pymysql import DATETIME
from sqlalchemy import Table

-- Suppliers Table
CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    Name VARCHAR(255),
    ContactInfo VARCHAR(255),
    DeliveryScheudle DATE,
    PerformanceRating DECIMAL (3, 2)
);

-- Inventory Table
CREATE TABLE Inventory (
    ItemID INT PRIMARY KEY,
    ItemName VARCHAR(255),
    Quantity INT,
    ReorderLevel INT,
    SupplierID INT,
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID)
);

-- Wines Table
CREATE TABLE Wines (
    WineID INT PRIMARY KEY,
    WineName VARCHAR(255),
    Type VARCHAR(50),
    Price DECIMAL (10, 2),
    Stock INT
);

-- Distributors Table
CREATE TABLE Distributors (
    DistributorID INT PRIMARY KEY,
    Name VARCHAR(255),
    ContactInfo VARCHAR(255),
    SalesQuota DECIMAL (10, 2),
);

-- Orders Table
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    Status VARCHAR(50),
    DistributorID INT,
    WineID INT,
    FOREIGN KEY (WineID) REFERENCES Wines(WineID)
    FOREIGN KEY (DistributorID) REFERENCES Distributors(DistributorID)
);

-- Employees Table
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    Name VARCHAR(255),
    Role VARCHAR(50),
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

-- Departments Table
CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(255),
);

-- TimeTracking Table
CREATE TABLE TimeTracking (
    TimeTrackingID INT PRIMARY KEY,
    EmployeeID INT,
    ClockIn DATETIME,
    ClockOut DATETIME,
    Date DATE,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);