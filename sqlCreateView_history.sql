-- View: public.peertopeercalls
-- DROP VIEW public.peertopeercalls;
CREATE OR REPLACE VIEW public."peertopeercalls_2023-03" AS
SELECT seg."CallId",
  seg."SessionId",
  sess."Modalities",
  -- sess."Caller",
  -- sess."Callee",
  -- caller."UPN" AS callerupn,
  -- callee."UPN" AS calleeupn,
  stream."StreamDirection",
  stream."MediaId",
  case
    when (
      cast(
        regexp_replace(
          stream."AverageRoundTripTime",
          '[A-Z,a-z]+',
          '',
          'g'
        ) as float
      ) * 1000
    ) <= 500
    and (
      cast(
        regexp_replace(stream."AverageJitter", '[A-Z,a-z]+', '', 'g') as float
      ) * 1000
    ) <= 30
    and (
      cast(stream."AveragePacketLossRate" as float) * 100
    ) <= 10 then 'Normal'
    else 'Poor'
  end as PCRLevel,
  device."DeviceType",
  device."RenderDeviceName",
  network."NetworkType",
  network."IPAddress",
  network."Subnet",
  network."ConnectionType",
  network."Port",
  network."ReflexiveIPAddress",
  network."RelayIPAddress",
  network."WifiBand",
  geos."Location"
FROM "History_PCRCallRecordDetails_2023-03" details
  LEFT JOIN "History_PCRCallRecordSegments_2023-03" seg ON seg."CallId" = details."Id"
  LEFT JOIN "History_PCRCallRecordSessions_2023-03" sess ON seg."CallId" = sess."CallId"
  LEFT JOIN "History_PCRCallRecordStreams_2023-03" stream ON stream."CallId" = sess."CallId"
  LEFT JOIN "History_PCRCallRecordDevices_2023-03" device ON device."MediaId" = stream."MediaId"
  LEFT JOIN "History_PCRCallRecordNetworks_2023-03" network ON stream."MediaId" = network."MediaId"
  LEFT JOIN "LicensedUsers" caller ON sess."Caller" = caller."Id"
  LEFT JOIN "LicensedUsers" callee ON sess."Callee" = callee."Id"
  LEFT JOIN "GeoIPs" geos ON regexp_replace(
    network."IPAddress"::text,
    '(\d{1,3}).(\d{1,3}).(\d{1,3}).*'::text,
    '\1.\2.\3.1'::text
  )::inet >= geos."IPStart"::inet
  AND regexp_replace(
    network."IPAddress"::text,
    '(\d{1,3}).(\d{1,3}).(\d{1,3}).*'::text,
    '\1.\2.\3.1'::text
  )::inet <= geos."IPEnd"::inet
WHERE details."CallType"::text = 'PeerToPeer'::text
  AND network."IPAddress"::text ~ '^(\d{1,3})\.(\d{1,3})\.'::text
  AND sess."Modalities"::text ~~ '%Audio%'::text;
ALTER TABLE public.peertopeercalls OWNER TO uccadmin;