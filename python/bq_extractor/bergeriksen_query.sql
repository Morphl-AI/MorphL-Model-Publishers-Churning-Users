SELECT
  clientId,
  sessions,
  bounces,
  noHits - pageViews as events,
  sessionDuration,
  pageViews,
  IF(device='mobile', 1, 0) isMobile,
  IF(device='desktop', 1, 0) isDesktop,
  IF(device='tablet', 1, 0) isTablet,
  IF(session_dates[SAFE_OFFSET(0)] IS NULL, 0, DATE_DIFF(CURRENT_DATE(), PARSE_DATE("%Y%m%d", session_dates[SAFE_OFFSET(0)]), DAY) ) days_since_last_session, 
  session_dates
FROM(
  SELECT
    clientId,
    SUM(totals.visits) as sessions,
    SUM(CASE WHEN totals.bounces IS NOT NULL THEN 1 ELSE 0 END) as bounces,
    SUM(CASE WHEN totals.hits IS NOT NULL THEN totals.hits ELSE 0 END) as noHits,
    SUM(CASE WHEN totals.timeOnSite IS NOT NULL THEN totals.timeOnSite ELSE 0 END) as sessionDuration,
    SUM(CASE WHEN totals.pageViews IS NOT NULL THEN totals.pageViews ELSE 0 END) as pageViews,
    ANY_VALUE(device.deviceCategory) as device,
    ARRAY_AGG(DISTINCT date ORDER BY date DESC) session_dates
  FROM morphl-212711.92806566.ga_sessions_20180918
  WHERE totals.visits = 1
    AND clientId IS NOT NULL
    AND ARRAY_LENGTH(ARRAY((SELECT DISTINCT page.hostname FROM UNNEST(hits) hits WHERE page.hostname = 'www.carolinebergeriksen.no'))) > 0
  GROUP BY clientId
  ORDER BY sessions ASC
) WHERE sessions > 1

