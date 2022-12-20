INSERT INTO f_azure(date_id, product_name, consumed_quantity, extended_cost, environment, module, poolName, meter_name, meter_category, meter_sub_category, unit_of_measure, resource_group, entity)
SELECT cast(substring(date, 1, 10) as date) date_id,
product,
SUM(quantity) as consumed_quantity,
SUM(cost) as extended_cost,
environment,
module,
poolName,
meter_name,
meter_category,
meter_sub_category,
unit_of_measure,
resource_group,
entity FROM r_azure
GROUP BY date, product, environment, entity, module, poolName, meter_name, meter_category, meter_sub_category, unit_of_measure, resource_group;