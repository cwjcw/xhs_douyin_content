-- 计算抖音 铂爵旅拍 账号的客资数量

DECLARE @yesterdayStart DATETIME;
DECLARE @yesterdayEnd DATETIME;

-- 计算昨天的起始时间和结束时间
SET @yesterdayStart = '2025-04-01 0:0:0'; -- 昨天的 00:00:00
--SET @yesterdayStart = DATEADD(day, -1, CAST(GETDATE() AS DATE)); -- 昨天的 00:00:00
--SET @yesterdayEnd = '2025-03-25 0:0:0'; -- 昨天的 00:00:00
SET @yesterdayEnd = DATEADD(SECOND, -1, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0));   -- 昨天的 23:59:59

-- 查看变量的值 (可以省略，用于调试)
--SELECT @yesterdayStart AS YesterdayStart, @yesterdayEnd AS YesterdayEnd;

SELECT
    CAST(cus.CreateTime AS DATE) AS '创建日期',
    '抖音' AS '所属平台',    
    COUNT(*) AS '客资数'
FROM
    bjCustomM cus
WHERE
    cus.CreateTime BETWEEN @yesterdayStart AND @yesterdayEnd
    AND cus.FromSourceId = 'B68'
GROUP BY
    CAST(cus.CreateTime AS DATE)
ORDER BY
    CAST(cus.CreateTime AS DATE) DESC



