-- use schema proddb;
create or replace table static.tbl_us_federal_holidays_snowflake (
  holiday_date date,
  holiday_name varchar
);


insert into static.tbl_us_federal_holidays_snowflake (holiday_date, holiday_name)
values
-- 2024
('2024-01-01', 'New Year''s Day'),
('2024-01-15', 'Martin Luther King Jr. Day'),
('2024-02-19', 'Presidents'' Day'),
('2024-05-27', 'Memorial Day'),
('2024-06-19', 'Juneteenth National Independence Day'),
('2024-07-04', 'Independence Day'),
('2024-09-02', 'Labor Day'),
('2024-10-14', 'Columbus Day'),
('2024-11-11', 'Veterans Day'),
('2024-11-28', 'Thanksgiving Day'),
('2024-12-25', 'Christmas Day'),

-- 2025
('2025-01-01', 'New Year''s Day'),
('2025-01-20', 'Martin Luther King Jr. Day'),
('2025-02-17', 'Presidents'' Day'),
('2025-05-26', 'Memorial Day'),
('2025-06-19', 'Juneteenth National Independence Day'),
('2025-07-04', 'Independence Day'),
('2025-09-01', 'Labor Day'),
('2025-10-13', 'Columbus Day'),
('2025-11-11', 'Veterans Day'),
('2025-11-27', 'Thanksgiving Day'),
('2025-12-25', 'Christmas Day'),

-- 2026
('2026-01-01', 'New Year''s Day'),
('2026-01-19', 'Martin Luther King Jr. Day'),
('2026-02-16', 'Presidents'' Day'),
('2026-05-25', 'Memorial Day'),
('2026-06-19', 'Juneteenth National Independence Day'),
('2026-07-03', 'Independence Day (Observed)'),
('2026-09-07', 'Labor Day'),
('2026-10-12', 'Columbus Day'),
('2026-11-11', 'Veterans Day'),
('2026-11-26', 'Thanksgiving Day'),
('2026-12-25', 'Christmas Day')
;
