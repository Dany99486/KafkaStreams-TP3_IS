-- Criação das tabelas
CREATE TABLE route_suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Inserção de dados nas tabelas
INSERT INTO route_suppliers (name)
VALUES 
('Supplier A'),
('Supplier B');