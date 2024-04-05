module StreamTests exposing (run)

import BackendTask exposing (BackendTask)
import BackendTaskTest exposing (testScript)
import Expect
import FatalError exposing (FatalError)
import Json.Decode as Decode
import Pages.Script as Script exposing (Script)
import Stream exposing (Stream)
import Test


run : Script
run =
    testScript "Stream"
        [ Stream.fromString "asdf\nqwer\n"
            |> Stream.captureCommandWithInput "wc" [ "-l" ]
            |> try
            |> test "capture stdin"
                (\output ->
                    output.stdout
                        |> String.trim
                        |> Expect.equal
                            "2"
                )
        , Stream.fromString "asdf\nqwer\n"
            |> Stream.runCommandWithInput "wc" [ "-l" ]
            |> try
            |> test "run stdin"
                (\() ->
                    Expect.pass
                )
        , Stream.fileRead "elm.json"
            |> Stream.pipe Stream.gzip
            |> Stream.pipe (Stream.fileWrite zipFile)
            |> Stream.run
            |> BackendTask.andThen
                (\() ->
                    Stream.fileRead zipFile
                        |> Stream.pipe Stream.unzip
                        |> Stream.readJson (Decode.field "type" Decode.string)
                )
            |> test "zip and unzip" (Expect.equal "application")
        , Stream.fromString
            """module            Foo
       
a = 1
b =            2
               """
            |> Stream.captureCommandWithInput "elm-format" [ "--stdin" ]
            |> try
            |> test "elm-format --stdin"
                (\{ stdout } ->
                    stdout
                        |> Expect.equal
                            """module Foo exposing (a, b)


a =
    1


b =
    2
"""
                )
        ]


test : String -> (a -> Expect.Expectation) -> BackendTask FatalError a -> BackendTask FatalError Test.Test
test name toExpectation task =
    task
        |> BackendTask.map
            (\data ->
                Test.test name <|
                    \() -> toExpectation data
            )


try : BackendTask { error | fatal : FatalError } data -> BackendTask FatalError data
try =
    BackendTask.allowFatal


zipFile : String.String
zipFile =
    "elm-review-report.gz.json"
