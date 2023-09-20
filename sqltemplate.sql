select seg."CallId",
    seg."SessionId",
    sess."Modalities",
    Sess."Caller",
    sess."Callee",
    caller."UPN" as CallerUPN,
    Callee."UPN" as CalleeUPN,
    stream."StreamDirection",
    stream."MediaId",
    stream."AverageRoundTripTime",
    stream."AverageJitter",
    stream."AveragePacketLossRate",
    stream."AverageAudioNetworkJitter",
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
from "#PCRCallRecordDetails" details
    left join "#PCRCallRecordSegments" seg on seg."CallId" = details."Id"
    left join "#PCRCallRecordSessions" sess on seg."CallId" = sess."CallId"
    left join "#PCRCallRecordStreams" stream on stream."CallId" = sess."CallId"
    left join "#PCRCallRecordDevices" device on device."MediaId" = stream."MediaId"
    left join "LicensedUsers" Caller on sess."Caller" = caller."Id"
    left join "LicensedUsers" Callee on sess."Callee" = callee."Id"
    left join "#PCRCallRecordNetworks" network on stream."MediaId" = network."MediaId"
    left join "GeoIPs" geos on inet(
        regexp_replace(
            network."IPAddress",
            '(\d{1,3}).(\d{1,3}).(\d{1,3}).*',
            '\1.\2.\3.1'
        )
    ) BETWEEN inet(geos."IPStart") and inet(geos."IPEnd")
WHERE details."CallType" = 'PeerToPeer'
    and network."IPAddress" ~ '^(\d{1,3})\.(\d{1,3})\.' --seg."CallId"='cb785e00-6a6b-4656-87e8-06a7790e6771'
    and Sess."Modalities" like '%Audio%';