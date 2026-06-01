module StaticResponsesTests exposing (all)

import BackendTask exposing (BackendTask)
import BackendTask.Http
import BuildError exposing (BuildError)
import Dict
import Expect
import FatalError exposing (FatalError)
import Json.Decode as Decode
import Json.Encode as Encode
import Pages.Internal.Platform.StaticResponses as StaticResponses exposing (NextStep(..))
import Pages.Internal.StaticHttpBody exposing (Body(..))
import Pages.Script as Script
import Pages.StaticHttp.Request as Request exposing (Request)
import RequestsAndPending exposing (ResponseBody)
import Server.Request
import Test exposing (Test, describe, test)


all : Test
all =
    describe "StaticResponses"
        [ test "simple get" <| \() ->
        BackendTask.Http.getJson "https://api.github.com/repos/dillonkearns/elm-pages"
            (Decode.field "stargazers_count" Decode.int)
            |> BackendTask.allowFatal
            |> expectRequestChain 123
                [ [ ( get "https://api.github.com/repos/dillonkearns/elm-pages"
                    , Encode.object
                        [ ( "stargazers_count", Encode.int 123 )
                        ]
                    )
                  ]
                ]
        , test "andThen" <| \() ->
        BackendTask.Http.getJson "https://api.github.com/repos/dillonkearns/elm-pages"
            (Decode.field "stargazers_count" Decode.int)
            |> BackendTask.andThen
                (\elmPagesStars ->
                    BackendTask.Http.getJson "https://api.github.com/repos/dillonkearns/elm-graphql"
                        (Decode.field "stargazers_count" Decode.int)
                        |> BackendTask.map (\graphqlStars -> elmPagesStars + graphqlStars)
                )
            |> BackendTask.allowFatal
            |> expectRequestChain 579
                [ [ ( get "https://api.github.com/repos/dillonkearns/elm-pages"
                    , Encode.object
                        [ ( "stargazers_count", Encode.int 123 )
                        ]
                    )
                  ]
                , [ ( get "https://api.github.com/repos/dillonkearns/elm-graphql"
                    , Encode.object
                        [ ( "stargazers_count", Encode.int 456 )
                        ]
                    )
                  ]
                ]
        , test "log" <| \() ->
        Script.log "Hello!"
            |> expectRequestChain ()
                [ [ ( log "Hello!"
                    , Encode.object []
                    )
                  ]
                ]
        , test "andThen log" <| \() ->
        BackendTask.Http.getJson "https://api.github.com/repos/dillonkearns/elm-pages"
            (Decode.field "stargazers_count" Decode.int)
            |> BackendTask.allowFatal
            |> BackendTask.andThen
                (\stars ->
                    Script.log ("Stars: " ++ String.fromInt stars)
                )
            |> expectRequestChain ()
                [ [ ( get "https://api.github.com/repos/dillonkearns/elm-pages"
                    , Encode.object
                        [ ( "stargazers_count", Encode.int 123 )
                        ]
                    )
                  ]
                , [ ( log "Stars: 123"
                    , Encode.object []
                    )
                  ]
                ]
        , describe "readBody BackendTask"
            [ test "readBody decodes a string body from the response" <|
                \() ->
                    Server.Request.readBody
                        |> BackendTask.map
                            (\maybeBody ->
                                case maybeBody of
                                    Just body ->
                                        body

                                    Nothing ->
                                        "NO BODY"
                            )
                        |> expectRequestChain "{\"token\":\"x\"}"
                            [ [ ( internalRequest "read-request-body"
                                , Encode.object [ ( "body", Encode.string "{\"token\":\"x\"}" ) ]
                                )
                              ]
                            ]
            , test "readBody returns Nothing when body is null" <|
                \() ->
                    Server.Request.readBody
                        |> BackendTask.map
                            (\maybeBody ->
                                case maybeBody of
                                    Just _ ->
                                        "HAS BODY"

                                    Nothing ->
                                        "NO BODY"
                            )
                        |> expectRequestChain "NO BODY"
                            [ [ ( internalRequest "read-request-body"
                                , Encode.object [ ( "body", Encode.null ) ]
                                )
                              ]
                            ]
            , test "readBody returns Nothing when body field is missing" <|
                \() ->
                    Server.Request.readBody
                        |> BackendTask.map
                            (\maybeBody ->
                                case maybeBody of
                                    Just _ ->
                                        "HAS BODY"

                                    Nothing ->
                                        "NO BODY"
                            )
                        |> expectRequestChain "NO BODY"
                            [ [ ( internalRequest "read-request-body"
                                , Encode.object []
                                )
                              ]
                            ]
            , test "readBody returns Nothing when body is a non-string value (Buffer serialization bug)" <|
                \() ->
                    -- This test reproduces the original bug: if JS sends a Buffer object
                    -- instead of a string, the decoder should return Nothing (not crash).
                    -- The fix is on the JS side (convert Buffer to string), but this test
                    -- verifies the Elm decoder is resilient to wrong types.
                    Server.Request.readBody
                        |> BackendTask.map
                            (\maybeBody ->
                                case maybeBody of
                                    Just _ ->
                                        "HAS BODY"

                                    Nothing ->
                                        "NO BODY"
                            )
                        |> expectRequestChain "NO BODY"
                            [ [ ( internalRequest "read-request-body"
                                , Encode.object
                                    [ ( "body"
                                      , Encode.object
                                            [ ( "type", Encode.string "Buffer" )
                                            , ( "data", Encode.list Encode.int [ 123, 34, 116, 34, 125 ] )
                                            ]
                                      )
                                    ]
                                )
                              ]
                            ]
            , test "readBody works with andThen to process the body" <|
                \() ->
                    Server.Request.readBody
                        |> BackendTask.andThen
                            (\maybeBody ->
                                case maybeBody of
                                    Just body ->
                                        Script.log ("Got body: " ++ body)

                                    Nothing ->
                                        Script.log "No body"
                            )
                        |> expectRequestChain ()
                            [ [ ( internalRequest "read-request-body"
                                , Encode.object [ ( "body", Encode.string "hello" ) ]
                                )
                              ]
                            , [ ( log "Got body: hello"
                                , Encode.object []
                                )
                              ]
                            ]
            ]
        ]


log : String -> Request
log message =
    portRequest "log"
        (Encode.object
            [ ( "message", Encode.string message )
            ]
        )


portRequest : String -> Encode.Value -> Request
portRequest portName body =
    { url = "elm-pages-internal://" ++ portName
    , method = "GET"
    , headers = []
    , body = JsonBody body
    , cacheOptions = Nothing
    , quiet = False
    , env = Dict.empty
    , dir = []
    }


get : String -> Request
get url =
    { url = url
    , method = "GET"
    , headers = []
    , body = EmptyBody
    , cacheOptions = Nothing
    , quiet = False
    , env = Dict.empty
    , dir = []
    }


internalRequest : String -> Request
internalRequest name =
    { url = "elm-pages-internal://" ++ name
    , method = "GET"
    , headers = []
    , body = EmptyBody
    , cacheOptions = Just (Encode.object [])
    , quiet = False
    , env = Dict.empty
    , dir = []
    }


expectRequestChain :
    a
    -> List (List ( Request, Encode.Value ))
    -> BackendTask FatalError a
    -> Expect.Expectation
expectRequestChain expectedValue expectedChain request =
    expectRequestChainHelp expectedValue
        (expectedChain |> List.map (List.map Tuple.first))
        (expectedChain
            |> List.map
                (List.map
                    (Tuple.mapFirst
                        (withInternalHeader
                            (RequestsAndPending.JsonBody Encode.null)
                        )
                    )
                )
        )
        []
        request
        RequestsAndPending.empty
        { errors = []
        }


expectRequestChainHelp :
    a
    -> List (List Request)
    -> List (List ( Request, Encode.Value ))
    -> List (List Request)
    -> BackendTask FatalError a
    -> RequestsAndPending.RequestsAndPending
    ->
        { errors : List BuildError
        }
    -> Expect.Expectation
expectRequestChainHelp expectedValue fullExpectedChain expectedChain chainSoFar backendTask responses values =
    case
        StaticResponses.nextStep responses backendTask values
    of
        Finish actualFinalValue ->
            Expect.all
                [ \() ->
                    chainSoFar
                        |> List.reverse
                        |> List.map (List.map .url)
                        |> Expect.equal (fullExpectedChain |> List.map (List.map .url))
                , \() ->
                    actualFinalValue
                        |> Expect.equal expectedValue
                ]
                ()

        FinishedWithErrors errors ->
            ("Expected no errors, got FinishedWithErrors: \n"
                ++ BuildError.errorsToString errors
            )
                |> Expect.fail

        Continue requests rawRequest ->
            case expectedChain of
                first :: rest ->
                    let
                        thing : RequestsAndPending.RequestsAndPending
                        thing =
                            { json =
                                first
                                    |> List.map
                                        (\( request, response ) ->
                                            ( Request.hash request
                                            , Encode.object
                                                [ ( "response"
                                                  , Encode.object
                                                        [ ( "body", response )
                                                        , ( "bodyKind", Encode.string "json" )
                                                        ]
                                                  )
                                                ]
                                            )
                                        )
                                    |> Encode.object
                            , rawBytes = Dict.empty
                            }
                    in
                    expectRequestChainHelp expectedValue
                        fullExpectedChain
                        rest
                        (requests :: chainSoFar)
                        rawRequest
                        thing
                        { errors = [] }

                _ ->
                    -- TODO give error because it's not complete but should be?
                    (requests :: chainSoFar)
                        |> List.reverse
                        |> List.map (List.map .url)
                        |> Expect.equal (fullExpectedChain |> List.map (List.map .url))


withInternalHeader : ResponseBody -> { a | headers : List ( String, String ) } -> { a | headers : List ( String, String ) }
withInternalHeader res req =
    { req
        | headers =
            ( "elm-pages-internal"
            , case res of
                RequestsAndPending.JsonBody _ ->
                    "ExpectJson"

                RequestsAndPending.BytesBody _ ->
                    "ExpectBytes"

                RequestsAndPending.StringBody _ ->
                    "ExpectString"

                RequestsAndPending.WhateverBody ->
                    "ExpectWhatever"
            )
                :: req.headers
    }
