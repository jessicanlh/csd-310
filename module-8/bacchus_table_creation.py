import mysql.connector
from mysql.connector import errorcode

#connect to mysql
def create_database():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='PASSWORD',
    )
    cursor = connection.cursor()

    #create the database
    cursor.execute('CREATE DATABASE IF NOT EXIST bacchus_winery')
    connection.close()
    print('Database created successfully')

#create user
def create_user():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='PASSWORD',
    )
    cursor = connection.cursor()

    cursor.execute("CREATE USER IF NOT EXISTS 'winery_user'@'localhost' IDENTIFIED BY 'wine'")
    cursor.execute("GRANT ALL PRIVILEGES ON bacchus_winery TO 'winery_user'@'localhost'")
    cursor.execute("FLUSH PRIVILEGES")

    connection.close()
    print('User created successfully')

def create_tables():
    connection = mysql.connector.connect(
        host='localhost',
        user='root]]',
        password='<PASSWORD>',
        database= 'bacchus_winery',
    )
    cursor = connection.cursor()

    #sql statements to create tables
    tables = [
        #Suppliers
        """
        CREATE TABLE IF NOT EXISTS Suppliers (
            SUPPLIERID INT AUTO_INCREMENT PRIMARY KEY,
            Name VARCHAR(255) NOT NULL,
            ContactInfo VARCHAR(255) NOT NULL,
            DeliverySchedule DATE, 
            PerformanceRating DECIMAL(5, 2)
        );
        """,

        #Inventory
        """
        CREATE TABLE IF NOT EXISTS Inventory (
            ItemID INT AUTO_INCREMENT PRIMARY KEY,
            ItemName VARCHAR(255) NOT NULL,
            Quantity INT, 
            ReorderLevel INT,
            SupplierID INT,
            FOREIGN KEY (SupplierID) REFERENCES Suppliers(SUPPLIERID)
        );
        """,

        #wines
        """
        CREATE TABLE IF NOT EXISTS Wines (
            WineID INT AUTO_INCREMENT PRIMARY KEY,
            WineName VARCHAR(255) NOT NULL,
            Type VARCHAR(50),
            Price DECIMAL(10, 2),
            Stock INT
        );
        """,

        #Distributors
        """
        CREATE TABLE IF NOT EXISTS Distributors (
            DistributorID INT AUTO_INCREMENT PRIMARY KEY,
            Name VARCHAR(255) NOT NULL,
            ContactInfo VARCHAR(255) NOT NULL,
            SalesQuota DECIMAL(10, 2)
        );
        """,

        #Orders
        """
        CREATE TABLE IF NOT EXISTS Orders (
            OrderID INT AUTO_INCREMENT PRIMARY KEY,
            OrderDate DATE NOT NULL,
            Status VARCHAR(255) NOT NULL,
            DistributorID INT,
            WineID INT,
            FOREIGN KEY (WineID) REFERENCES Wines(WineID),
            FOREIGN KEY (DistributorID) REFERENCES Distributors(DistributorID)
        );
        """,

        #Employees
        """
        CREATE TABLE IF NOT EXISTS Employees (
            EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
            Name VARCHAR(255) NOT NULL,
            Role VARCHAR(255) NOT NULL,
            DepartmentID INT, 
            FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
        );
        """,

        #departments
        """
        CREATE TABLE IF NOT EXISTS Departments (
            DepartmentID INT AUTO_INCREMENT PRIMARY KEY,
            DepartmentName VARCHAR(255) NOT NULL
        );
        """,

        #time tracking
        """
        CREATE TABLE IF NOT EXISTS TimeTracking (
            TrackingID INT AUTO_INCREMENT PRIMARY KEY,
            EmployeeID INT, 
            Quarter1Hours DECIMAL(10, 2),
            Quarter2Hours DECIMAL(10, 2),
            Quarter3Hours DECIMAL(10, 2),
            Quarter4Hours DECIMAL(10, 2),
            TotalHours DECIMAL(10, 2) GENERATED ALWAYS AS (
                Quarter1Hours + Quarter2Hours + Quarter3Hours + Quarter4Hours
            ) STORED,
            FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
        );
        """
    ]
    #execute each SQL statement
    for table in tables:
        cursor.execute(table)

    #commit changes and close the connection
    connection.commit()
    connection.close()

    print('Tables created successfully')

#run the script
if __name__ == '__main__':
    create_database()
    create_tables()