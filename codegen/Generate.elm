port module Generate exposing (main)

{-| -}

import Elm exposing (File)
import Elm.Annotation
import Elm.Case
import Elm.CodeGen
import Elm.Declare
import Elm.Op
import Elm.Pretty
import Gen.Basics
import Gen.CodeGen.Generate exposing (Error)
import Gen.List
import Gen.Path
import Gen.Server.Response
import Gen.String
import Pages.Internal.RoutePattern as RoutePattern exposing (RoutePattern)
import Pretty


type alias Flags =
    { templates : List (List String)
    }


main : Program Flags () ()
main =
    Platform.worker
        { init =
            \{ templates } ->
                ( ()
                , onSuccessSend [ file templates ]
                )
        , update =
            \_ model ->
                ( model, Cmd.none )
        , subscriptions = \_ -> Sub.none
        }


splitPath : { declaration : Elm.Declaration, call : Elm.Expression -> Elm.Expression, callFrom : List String -> Elm.Expression -> Elm.Expression }
splitPath =
    Elm.Declare.fn "splitPath"
        ( "path", Just Gen.Path.annotation_.path )
        (\path ->
            Gen.List.call_.filter
                (Elm.fn ( "item", Just Elm.Annotation.string )
                    (\item -> Elm.Op.notEqual item (Elm.string ""))
                )
                (Gen.String.call_.split (Elm.string "/") path)
        )


maybeToList : { declaration : Elm.Declaration, call : Elm.Expression -> Elm.Expression, callFrom : List String -> Elm.Expression -> Elm.Expression }
maybeToList =
    Elm.Declare.fn "maybeToList"
        ( "maybeString", Just (Elm.Annotation.maybe Elm.Annotation.string) )
        (\maybeString ->
            Elm.Case.maybe maybeString
                { nothing = Elm.list []
                , just = ( "string", \string -> Elm.list [ string ] )
                }
                |> Elm.withType (Elm.Annotation.list Elm.Annotation.string)
        )


segmentsToRoute :
    List RoutePattern
    -> { declaration : Elm.Declaration, call : Elm.Expression -> Elm.Expression, callFrom : List String -> Elm.Expression -> Elm.Expression }
segmentsToRoute routes =
    Elm.Declare.fn "segmentsToRoute"
        ( "segments"
        , Elm.Annotation.list Elm.Annotation.string |> Just
        )
        (\segments ->
            (((routes
                |> List.concatMap RoutePattern.routeToBranch
                |> List.map (Tuple.mapSecond (\constructRoute -> Elm.CodeGen.apply [ Elm.CodeGen.val "Just", constructRoute ]))
              )
                ++ [ ( Elm.CodeGen.allPattern, Elm.CodeGen.val "Nothing" )
                   ]
             )
                |> Elm.CodeGen.caseExpr (Elm.CodeGen.val "segments")
            )
                |> Elm.Pretty.prettyExpression
                |> Pretty.pretty 120
                |> Elm.val
                |> Elm.withType
                    (Elm.Annotation.named [] "Route"
                        |> Elm.Annotation.maybe
                    )
        )


routeToPath : List RoutePattern -> { declaration : Elm.Declaration, call : Elm.Expression -> Elm.Expression, callFrom : List String -> Elm.Expression -> Elm.Expression }
routeToPath routes =
    Elm.Declare.fn "routeToPath"
        ( "route", Just (Elm.Annotation.named [] "Route") )
        (\route_ ->
            Elm.Case.custom route_
                (Elm.Annotation.list Elm.Annotation.string)
                (routes
                    |> List.map
                        (\route ->
                            case
                                RoutePattern.toVariantName route
                                    |> .params
                                    |> List.filter
                                        (\param ->
                                            case param of
                                                RoutePattern.StaticParam _ ->
                                                    False

                                                _ ->
                                                    True
                                        )
                            of
                                [] ->
                                    Elm.Case.branch0 (RoutePattern.toVariantName route |> .variantName)
                                        (RoutePattern.toVariantName route
                                            |> .params
                                            |> List.map
                                                (\param ->
                                                    case param of
                                                        RoutePattern.StaticParam name ->
                                                            [ Elm.string name ]
                                                                |> Elm.list

                                                        RoutePattern.DynamicParam name ->
                                                            Elm.list []

                                                        RoutePattern.OptionalParam2 name ->
                                                            Elm.list []
                                                )
                                            |> Elm.list
                                        )

                                nonEmptyDynamicParams ->
                                    Elm.Case.branch1 (RoutePattern.toVariantName route |> .variantName)
                                        ( "params", Elm.Annotation.record [] )
                                        (\params ->
                                            RoutePattern.toVariantName route
                                                |> .params
                                                |> List.map
                                                    (\param ->
                                                        case param of
                                                            RoutePattern.StaticParam name ->
                                                                [ Elm.string name ]
                                                                    |> Elm.list

                                                            RoutePattern.DynamicParam name ->
                                                                [ Elm.get name params ]
                                                                    |> Elm.list

                                                            RoutePattern.OptionalParam2 name ->
                                                                maybeToList.call (Elm.get name params)
                                                    )
                                                |> Elm.list
                                        )
                        )
                )
                |> Gen.List.call_.concat
                |> Elm.withType (Elm.Annotation.list Elm.Annotation.string)
        )


file : List (List String) -> Elm.File
file templates =
    let
        routes : List RoutePattern.RoutePattern
        routes =
            templates
                |> List.filterMap RoutePattern.fromModuleName

        segmentsToRouteFn : { declaration : Elm.Declaration, call : Elm.Expression -> Elm.Expression, callFrom : List String -> Elm.Expression -> Elm.Expression }
        segmentsToRouteFn =
            segmentsToRoute routes
    in
    Elm.file
        [ "Route" ]
        [ Elm.customType "Route"
            (routes |> List.map RoutePattern.toVariant)
            |> expose
        , segmentsToRouteFn.declaration |> expose
        , splitPath.declaration
        , Elm.declaration "urlToRoute"
            (Elm.fn
                ( "url"
                , Elm.Annotation.extensible "url" [ ( "path", Elm.Annotation.string ) ]
                    |> Just
                )
                (\url ->
                    segmentsToRouteFn.call
                        (splitPath.call
                            (url |> Elm.get "path")
                        )
                        |> Elm.withType (Elm.Annotation.maybe (Elm.Annotation.named [] "Route"))
                )
            )
            |> expose
        , Elm.declaration "baseUrl" (Elm.string "/")
            |> expose
        , maybeToList.declaration
        , routeToPath routes |> .declaration |> expose
        , Elm.declaration "baseUrlAsPath"
            (Gen.List.call_.filter
                (Elm.fn ( "item", Nothing )
                    (\item ->
                        Gen.Basics.call_.not
                            (Gen.String.call_.isEmpty item)
                    )
                )
                (Gen.String.call_.split (Elm.string "/")
                    (Elm.val "baseUrl")
                )
            )
            |> expose
        , Elm.declaration "toPath"
            (Elm.fn ( "route", Elm.Annotation.named [] "Route" |> Just )
                (\route ->
                    Gen.Path.call_.fromString
                        (Gen.String.call_.join
                            (Elm.string "/")
                            (Elm.Op.append
                                (Elm.val "baseUrlAsPath")
                                (Elm.apply (Elm.val "routeToPath")
                                    [ route ]
                                )
                            )
                        )
                        |> Elm.withType (Elm.Annotation.named [ "Path" ] "Path")
                )
            )
            |> expose
        , toString.declaration
            |> expose
        , Elm.declaration "redirectTo"
            (Elm.fn ( "route", Elm.Annotation.named [] "Route" |> Just )
                (\route ->
                    Gen.Server.Response.call_.temporaryRedirect
                        (toString.call route)
                        |> Elm.withType
                            (Elm.Annotation.namedWith [ "Server", "Response" ]
                                "Response"
                                [ Elm.Annotation.var "data"
                                , Elm.Annotation.var "error"
                                ]
                            )
                )
            )
            |> expose
        ]


toString : { declaration : Elm.Declaration, call : Elm.Expression -> Elm.Expression, callFrom : List String -> Elm.Expression -> Elm.Expression }
toString =
    Elm.Declare.fn "toString"
        ( "route", Elm.Annotation.named [] "Route" |> Just )
        (\route ->
            Gen.Path.toAbsolute
                (Elm.apply (Elm.val "toPath") [ route ])
        )


expose : Elm.Declaration -> Elm.Declaration
expose declaration =
    declaration
        |> Elm.exposeWith
            { exposeConstructor = True
            , group = Nothing
            }


port onSuccessSend : List File -> Cmd msg


port onFailureSend : List Error -> Cmd msg


port onInfoSend : String -> Cmd msg
