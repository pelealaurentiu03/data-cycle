-- First, make sure you're not using the database you want to drop
USE master;
GO

-- Close existing connections to the database before dropping
ALTER DATABASE Apartment SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

-- Drop the database if it exists
DROP DATABASE IF EXISTS Apartment;
GO

-- Create the database
CREATE DATABASE Apartment;
GO

-- Use the database
USE Apartment;
GO

CREATE TABLE DimDate (
    idDate INT PRIMARY KEY IDENTITY(1,1),
    year INT,
    month INT,
    day INT
);

CREATE TABLE DimTime (
    idTime INT PRIMARY KEY IDENTITY(1,1),
    hour INT,
    minute INT
);

CREATE TABLE DimLocation (
    idLocation INT PRIMARY KEY IDENTITY(1,1),
    siteName VARCHAR(50)
);

CREATE TABLE DimMeasurement (
    idMeasurement INT PRIMARY KEY IDENTITY(1,1),
    measurement VARCHAR(50),
    unit VARCHAR(50)
);

CREATE TABLE DimRoom (
    idRoom INT PRIMARY KEY IDENTITY(1,1),
    roomName VARCHAR(50)
);

CREATE TABLE DimSensor (
    idSensor INT PRIMARY KEY IDENTITY(1,1),
    sensorType VARCHAR(50)
);

CREATE TABLE DimBuilding (
    idBuilding INT PRIMARY KEY IDENTITY(1,1),
    buildingType VARCHAR(50),
    houseName VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    adress VARCHAR(50),
    npa VARCHAR(50),
    city VARCHAR(50),
    nbPeople INT,
    isHeatingOn BIT
);

CREATE TABLE Fact_WeatherPrediction (
    idDate INT,
    idTime INT,
    idMeasurement INT,
    idLocation INT,
    valueMeasurement FLOAT,
    PRIMARY KEY (idDate, idTime, idMeasurement, idLocation),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime),
    FOREIGN KEY (idMeasurement) REFERENCES DimMeasurement(idMeasurement),
    FOREIGN KEY (idLocation) REFERENCES DimLocation(idLocation)
);


CREATE TABLE Fact_Plugs (
    idBuilding INT,
    idRoom INT,
    idSensor INT,
    idDate INT,
    idTime INT,
    switch BIT,
    temperature FLOAT,
    overTemperature BIT,
    counter1 FLOAT,
    counter2 FLOAT,
    counter3 FLOAT,
    power FLOAT,
    overPower FLOAT,
    timeplug INT,
    total INT,
    PRIMARY KEY (idBuilding, idRoom, idSensor, idDate, idTime),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idSensor) REFERENCES DimSensor(idSensor),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime)
);

CREATE TABLE Fact_Humidities (
    idBuilding INT,
    idRoom INT,
    idSensor INT,
    idDate INT,
    idTime INT,
    temperature FLOAT,
    humidity FLOAT,
    devicePower INT,
    PRIMARY KEY (idBuilding, idRoom, idSensor, idDate, idTime),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idSensor) REFERENCES DimSensor(idSensor),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime)
);

CREATE TABLE Fact_Motions (
    idBuilding INT,
    idRoom INT,
    idSensor INT,
    idDate INT,
    idTime INT,
    motion BIT,
    light INT,
    temperature FLOAT,
    PRIMARY KEY (idBuilding, idRoom, idSensor, idDate, idTime),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idSensor) REFERENCES DimSensor(idSensor),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime)
);

CREATE TABLE Fact_MachineLearning (
    idDate INT,
    idTime INT,
    idBuilding INT,
    idRoom INT,
    ForecastDate DATE,
    ForecastType VARCHAR(50),
    ForecastValue FLOAT,
    PRIMARY KEY (idDate, idTime, idRoom, idBuilding, ForecastDate),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding)
);

CREATE TABLE Fact_DoorsWindows (
    idBuilding INT,
    idRoom INT,
    idSensor INT,
    idDate INT,
    idTime INT,
    doorsWindowsType VARCHAR(50),
    battery INT,
    defense BIT,
    switch BIT,
    PRIMARY KEY (idBuilding, idRoom, idSensor, idDate, idTime, doorsWindowsType),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idSensor) REFERENCES DimSensor(idSensor),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime)
);

CREATE TABLE Fact_Consumptions (
    idBuilding INT,
    idRoom INT,
    idSensor INT,
    idDate INT,
    idTime INT,
    isValid1 BIT,
    isValid2 BIT,
    isValid3 BIT,
    current1 FLOAT,
    current2 FLOAT,
    current3 FLOAT,
    power1 FLOAT,
    power2 FLOAT,
    power3 FLOAT,
    pf1 FLOAT,
    pf2 FLOAT,
    pf3 FLOAT,
    voltage1 FLOAT,
    voltage2 FLOAT,
    voltage3 FLOAT,
    switch BIT,
    PRIMARY KEY (idBuilding, idRoom, idSensor, idDate, idTime),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idSensor) REFERENCES DimSensor(idSensor),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime)
);

CREATE TABLE Fact_Meteos (
    idBuilding INT,
    idRoom INT,
    idSensor INT,
    idDate INT,
    idTime INT,
    humidity INT,
    temperature FLOAT,
    co2 INT,
    batteryPercent INT,
    noise INT,
    pressure FLOAT,
    absolutePressure FLOAT,
    PRIMARY KEY (idBuilding, idRoom, idSensor, idDate, idTime),
    FOREIGN KEY (idBuilding) REFERENCES DimBuilding(idBuilding),
    FOREIGN KEY (idRoom) REFERENCES DimRoom(idRoom),
    FOREIGN KEY (idSensor) REFERENCES DimSensor(idSensor),
    FOREIGN KEY (idDate) REFERENCES DimDate(idDate),
    FOREIGN KEY (idTime) REFERENCES DimTime(idTime)
);