create schema if not exists analytics;

create table analytics.dim_cliente as
select
    id_cliente,
    nome_cliente
from trusted.clientes;

create table analytics.dim_produto as
select
    p.id_produto,
    p.nome_produto,
    c.nome_categoria
from trusted.produtos p
join trusted.categorias c 
    on p.id_categoria = c.id_categoria;

create table analytics.dim_tempo as
select distinct
    data_venda,
    extract(year from data_venda) as ano,
    extract(month from data_venda) as mes
from trusted.vendas;

create table analytics.fato_vendas as
select
    v.id_venda,
    v.data_venda,
    v.id_cliente,
    i.id_produto,
    i.quantidade,
    i.valor_unitario,
    (i.quantidade * i.valor_unitario) as valor_total
from trusted.vendas v
join trusted.itens_venda i 
    on v.id_venda = i.id_venda;
