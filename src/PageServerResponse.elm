module PageServerResponse exposing (PageServerResponse(..), Response, StreamingResponseData, toJson, toRedirect, streamingResponseToJson)

import Bytes exposing (Bytes)
import Dict
import Json.Encode
import List.Extra


type PageServerResponse data error
    = RenderPage
        { statusCode : Int
        , headers : List ( String, String )
        }
        data
    | ServerResponse Response
    | StreamingServerResponse StreamingResponseData
    | ErrorPage error { headers : List ( String, String ) }


{-| Data for a streaming response. The `streamPipeline` is a JSON-encoded stream
pipeline definition (same format as BackendTask.Stream internals) that will be
executed on the JS side and piped to the HTTP response.
-}
type alias StreamingResponseData =
    { statusCode : Int
    , headers : List ( String, String )
    , streamPipeline : Json.Encode.Value
    }


streamingResponseToJson : StreamingResponseData -> Json.Encode.Value
streamingResponseToJson streamingResponse =
    Json.Encode.object
        [ ( "statusCode", Json.Encode.int streamingResponse.statusCode )
        , ( "headers"
          , streamingResponse.headers
                |> collectMultiValueHeaders
                |> List.map (Tuple.mapSecond (Json.Encode.list Json.Encode.string))
                |> Json.Encode.object
          )
        , ( "kind", Json.Encode.string "streaming-server-response" )
        , ( "streamPipeline", streamingResponse.streamPipeline )
        ]


toRedirect :
    { response
        | statusCode : Int
        , headers : List ( String, String )
    }
    -> Maybe { statusCode : Int, location : String }
toRedirect response =
    response.headers
        |> Dict.fromList
        |> Dict.get "Location"
        |> Maybe.andThen
            (\location ->
                if response.statusCode == 302 then
                    Just { statusCode = 302, location = location }

                else
                    Nothing
            )


type alias Response =
    { statusCode : Int
    , headers : List ( String, String )
    , body : Maybe String
    , bodyBytes : Maybe Bytes
    , isBase64Encoded : Bool
    }


toJson : Response -> Json.Encode.Value
toJson serverResponse =
    Json.Encode.object
        [ ( "body", serverResponse.body |> Maybe.map Json.Encode.string |> Maybe.withDefault Json.Encode.null )
        , ( "statusCode", serverResponse.statusCode |> Json.Encode.int )
        , ( "headers"
          , serverResponse.headers
                |> collectMultiValueHeaders
                |> List.map (Tuple.mapSecond (Json.Encode.list Json.Encode.string))
                |> Json.Encode.object
          )
        , ( "kind", Json.Encode.string "server-response" )
        , ( "isBase64Encoded", Json.Encode.bool serverResponse.isBase64Encoded )
        ]


collectMultiValueHeaders : List ( String, String ) -> List ( String, List String )
collectMultiValueHeaders headers =
    headers
        |> List.Extra.groupWhile
            (\( key1, _ ) ( key2, _ ) -> key1 == key2)
        |> List.map
            (\( ( key, firstValue ), otherValues ) ->
                ( key
                , firstValue
                    :: (otherValues |> List.map Tuple.second)
                )
            )
