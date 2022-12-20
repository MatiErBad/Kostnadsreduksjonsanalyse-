INSERT INTO f_oci (date_id, service, compartmentId, compartmentName, billedQuantity, unitPrice, myCost, entity, instance, module, instanceconfiguration, free_tier_retained)
SELECT cast(substring(intervalUsageStart, 1, 10) as date) date_id,
service,
compartmentId,
compartmentName,
SUM(billedQuantity) as billedQuantity,
SUM(unitPrice) as unitPrice,
SUM(myCost) as myCost,
entity,
instance,
module,
instanceconfiguration,
free_tier_retained FROM r_oci
GROUP BY intervalUsageStart, service, compartmentId, compartmentName, entity, instance, module, instanceconfiguration, free_tier_retained;