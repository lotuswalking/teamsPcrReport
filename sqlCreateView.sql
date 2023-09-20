-- View: public.peertopeercalls
-- DROP VIEW public.peertopeercalls;
CREATE OR REPLACE VIEW public.peertopeercalls AS
select seg."CallId",
  seg."SessionId",
  -- sess."Modalities",
  -- Sess."Caller", sess."Callee",
  -- caller."UPN" as CallerUPN, Callee."UPN" as CalleeUPN,
  stream."StreamDirection",
  -- stream."MediaId",
  -- stream."AverageRoundTripTime",
  -- stream."AverageJitter",
  -- stream."AveragePacketLossRate",
  -- stream."AverageAudioNetworkJitter",
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
  -- device."DeviceType",
  -- device."RenderDeviceName",
  -- network."NetworkType",
  network."IPAddress",
  -- network."Subnet",
  -- network."ConnectionType",
  -- network."Port",
  -- network."ReflexiveIPAddress",
  -- network."RelayIPAddress",
  -- network."WifiBand",
  case
    when geos."Location" ISNULL then 'Internet'
    ELSE geos."Location"
  end as Location,
  'eoq' as EOQ
from "PCRCallRecordDetails" details
  left join "PCRCallRecordSegments" seg on seg."CallId" = details."Id"
  left join "PCRCallRecordSessions" sess on seg."CallId" = sess."CallId"
  left join public."LicensedUsers" Caller on sess."Caller" = caller."Id"
  left join public."LicensedUsers" Callee on sess."Callee" = callee."Id"
  left join "PCRCallRecordStreams" stream on stream."CallId" = sess."CallId"
  left join "PCRCallRecordDevices" device on device."MediaId" = stream."MediaId"
  and left(device."DeviceType", 6) = left(stream."StreamDirection", 6)
  left join "PCRCallRecordNetworks" network on stream."MediaId" = network."MediaId"
  and left(network."NetworkType", 6) = left(stream."StreamDirection", 6)
  left join "GeoIPs" geos on inet(
    regexp_replace(
      network."IPAddress",
      '(\d{1,3}).(\d{1,3}).(\d{1,3}).*',
      '\1.\2.\3.1'
    )
  ) BETWEEN inet(geos."IPStart") and inet(geos."IPEnd")
WHERE details."CallType"::text = 'PeerToPeer'::text
  AND network."IPAddress"::text ~ '^(\d{1,3})\.(\d{1,3})\.'::text
  AND sess."Modalities"::text ~~ '%Audio%'::text;
ALTER TABLE public.peertopeercalls OWNER TO uccadmin;