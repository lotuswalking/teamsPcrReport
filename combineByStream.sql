with callerStream as (
   SELECT stream.*
   from "PCRCallRecordStreams" stream
      join "PCRCallRecordDetails" details on details."Id" = stream."CallId"
   where details."Modalities" ~ '^Audio'
      and details."CallType" = 'PeerToPeer'
      and stream."StreamDirection" = 'CallerToCallee'
),
calleeStream as (
   SELECT stream.*
   from "PCRCallRecordStreams" stream
      join "PCRCallRecordDetails" details on details."Id" = stream."CallId"
   where details."Modalities" ~ '^Audio'
      and details."CallType" = 'PeerToPeer'
      and stream."StreamDirection" = 'CalleeToCaller'
),
Callernetwork as (
   select "MediaId",
      "NetworkType",
      "IPAddress"
   from "PCRCallRecordNetworks"
   WHERE "NetworkType" = 'CallerNetwork'
      and "IPAddress" ~ '^(\d{1,3})\.(\d{1,3})\.'
),
Calleenetwork as (
   select "MediaId",
      "NetworkType",
      "IPAddress"
   from "PCRCallRecordNetworks"
   WHERE "NetworkType" = 'CalleeNetwork'
      and "IPAddress" ~ '^(\d{1,3})\.(\d{1,3})\.'
)
select callerStream."CallId",
   callerStream."MediaId",
   callerStream."StartDateTime",
   callerStream."EndDateTime",
   -- callerStream."UserId" as CallerID,
   callerStream."Location" as CallerLocation,
   -- callerStream."Country" as CallerCountry,
   -- calleeStream."UserId" as CalleeID,
   calleeStream."Location" as CalleeLocation,
   -- calleeStream."Country" as CalleeCountry,
   case
      when (
         cast(
            regexp_replace(
               callerStream."AverageRoundTripTime",
               '[A-Z,a-z]+',
               '',
               'g'
            ) as float
         ) * 1000
      ) <= 500
      and (
         cast(
            regexp_replace(callerStream."AverageJitter", '[A-Z,a-z]+', '', 'g') as float
         ) * 1000
      ) <= 30
      and (
         cast(callerStream."AveragePacketLossRate" as float) * 100
      ) <= 10 then 'Normal'
      else 'Poor'
   end as CallerPCRLevel,
   case
      when (
         cast(
            regexp_replace(
               calleeStream."AverageRoundTripTime",
               '[A-Z,a-z]+',
               '',
               'g'
            ) as float
         ) * 1000
      ) <= 500
      and (
         cast(
            regexp_replace(calleeStream."AverageJitter", '[A-Z,a-z]+', '', 'g') as float
         ) * 1000
      ) <= 30
      and (
         cast(calleeStream."AveragePacketLossRate" as float) * 100
      ) <= 10 then 'Normal'
      else 'Poor'
   end as CalleePCRLevel,
   case
      when Callergeos."Location" ISNULL then 'Internet'
      ELSE Callergeos."Location"
   end as CallerGeoLocation,
   case
      when Calleegeos."Location" ISNULL then 'Internet'
      ELSE Calleegeos."Location"
   end as CalleeGeoLocation,
   Callernetwork."IPAddress" as CallerIP,
   Calleenetwork."IPAddress" as CalleeIP,
   'end' as endCol
from callerStream
   left join calleeStream on callerStream."MediaId" = calleeStream."MediaId"
   left join Callernetwork on callerStream."MediaId" = Callernetwork."MediaId"
   and left(Callernetwork."NetworkType", 6) = left(callerStream."StreamDirection", 6)
   left join Calleenetwork on calleeStream."MediaId" = Calleenetwork."MediaId"
   and left(Calleenetwork."NetworkType", 6) = left(calleeStream."StreamDirection", 6)
   left join "GeoIPs" Callergeos on inet(
      regexp_replace(
         Callernetwork."IPAddress",
         '(\d{1,3}).(\d{1,3}).(\d{1,3}).*',
         '\1.\2.\3.1'
      )
   ) BETWEEN inet(Callergeos."IPStart") and inet(Callergeos."IPEnd")
   left join "GeoIPs" Calleegeos on inet(
      regexp_replace(
         Calleenetwork."IPAddress",
         '(\d{1,3}).(\d{1,3}).(\d{1,3}).*',
         '\1.\2.\3.1'
      )
   ) BETWEEN inet(Calleegeos."IPStart") and inet(Calleegeos."IPEnd")
order by callerStream."CallId",
   callerStream."MediaId";