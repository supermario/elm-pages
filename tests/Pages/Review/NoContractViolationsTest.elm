module Pages.Review.NoContractViolationsTest exposing (all)

import Pages.Review.NoContractViolations exposing (rule)
import Review.Test
import Test exposing (Test, describe, test)


all : Test
all =
    describe "Pages.Review.NoContractViolations"
        [ test "reports error when missing exposed declaration" <|
            \() ->
                """module Route.Blog.Slug_ exposing (Data, Msg, route)

a = 1
"""
                    |> testRouteModule
                    |> Review.Test.expectErrorsForModules
                        [ ( "Route.Blog.Slug_"
                          , [ Review.Test.error
                                { message = "Unexposed Declaration in Route Module"
                                , details =
                                    [ """Route Modules need to expose the following values:

- route
- Data
- Model
- Msg

But it is not exposing: Model"""
                                    ]
                                , under = "exposing (Data, Msg, route)"
                                }
                            ]
                          )
                        ]
        , test "reports RouteParams mismatch" <|
            \() ->
                """module Route.Blog.Slug_ exposing (Data, route, Model, Msg)

type alias RouteParams = { blogPostName : String }

route = {}
"""
                    |> testRouteModule
                    |> Review.Test.expectErrorsForModules
                        [ ( "Route.Blog.Slug_"
                          , [ Review.Test.error
                                { message = "RouteParams don't match Route Module name"
                                , details =
                                    [ """Expected

type alias RouteParams = { slug : String }
"""
                                    ]
                                , under = "{ blogPostName : String }"
                                }
                            ]
                          )
                        ]
        , test "reports incorrect types for optional RouteParams" <|
            \() ->
                """module Route.Docs.Section_.SubSection__ exposing (Data, route, Model, Msg)

type alias RouteParams = { section : String, subSection : String }

route = {}
"""
                    |> testRouteModule
                    |> Review.Test.expectErrorsForModules
                        [ ( "Route.Docs.Section_.SubSection__"
                          , [ Review.Test.error
                                { message = "RouteParams don't match Route Module name"
                                , details =
                                    [ """Expected

type alias RouteParams = { section : String, subSection : Maybe String }
"""
                                    ]
                                , under = "{ section : String, subSection : String }"
                                }
                            ]
                          )
                        ]
        , test "reports incorrect types for required splat RouteParams" <|
            \() ->
                """module Route.Docs.Section_.SPLAT_ exposing (Data, route, Model, Msg)

type alias RouteParams = { section : String, splat : List String }

route = {}
"""
                    |> testRouteModule
                    |> Review.Test.expectErrorsForModules
                        [ ( "Route.Docs.Section_.SPLAT_"
                          , [ Review.Test.error
                                { message = "RouteParams don't match Route Module name"
                                , details =
                                    [ """Expected

type alias RouteParams = { section : String, splat : ( String, List String ) }
"""
                                    ]
                                , under = "{ section : String, splat : List String }"
                                }
                            ]
                          )
                        ]
        , test "no error for valid SPLAT_ RouteParams" <|
            \() ->
                """module Route.Docs.Section_.SPLAT_ exposing (Data, route, Model, Msg)

type alias RouteParams = { section : String, splat : ( String, List String ) }

route = {}
                        """
                    |> testRouteModule
                    |> Review.Test.expectNoErrors
        , test "no error for valid SPLAT__ RouteParams" <|
            \() ->
                """module Route.Docs.Section_.SPLAT__ exposing (Data, route, Model, Msg)

type alias RouteParams = { section : String, splat : List String }

route = {}
                        """
                    |> testRouteModule
                    |> Review.Test.expectNoErrors
        , test "no error for matching RouteParams name" <|
            \() ->
                """module Route.Blog.Slug_ exposing (Data, route, Model, Msg)

type alias RouteParams = { slug : String }

route = {}
"""
                    |> testRouteModule
                    |> Review.Test.expectNoErrors
        , test "error when RouteParams type is not a record" <|
            \() ->
                """module Route.Blog.Slug_ exposing (Data, route, Model, Msg)

type alias RouteParams = ()

route = {}
"""
                    |> testRouteModule
                    |> Review.Test.expectErrorsForModules
                        [ ( "Route.Blog.Slug_"
                          , [ Review.Test.error
                                { message = "RouteParams must be a record type alias."
                                , details =
                                    [ """Expected a record type alias."""
                                    ]
                                , under = "()"
                                }
                            ]
                          )
                        ]
        , test "no error for modules that don't start with Route prefix" <|
            \() ->
                """module NotRouteModule.Blog.Slug_ exposing (Model, Msg)

type alias RouteParams = ()

route = {}
"""
                    |> testRouteModule
                    |> Review.Test.expectNoErrors
        , test "error for missing application module definitions" <|
            \() ->
                [ """module Route.Index exposing (Data, route, Model, Msg)

type alias RouteParams = {}

route = {}
"""
                , """module Site exposing (config)

config : SiteConfig
config =
    { canonicalUrl = canonicalUrl
    , head = head
    }
"""
                ]
                    |> Review.Test.runOnModules rule
                    |> Review.Test.expectGlobalErrors
                        [ { message = "Missing core modules"
                          , details =
                                [ "Api"
                                , "Effect"
                                , "Shared"
                                , "View"
                                ]
                          }
                        ]
        , test "no error when all core modules are defined" <|
            \() ->
                [ """module Route.Index exposing (Data, route, Model, Msg)
                            
type alias RouteParams = {}

route = {}
"""
                , """module Site exposing (config)
                            
config : SiteConfig
config =
    { canonicalUrl = canonicalUrl
    , head = head
    }
"""
                , """module Api exposing (routes)
routes = Debug.todo ""
"""
                , """module Effect exposing (routes)
routes = Debug.todo ""
"""
                , """module Shared exposing (routes)
routes = Debug.todo ""
"""
                , """module View exposing (routes)
routes = Debug.todo ""
"""
                ]
                    |> Review.Test.runOnModules rule
                    |> Review.Test.expectNoErrors
        ]


testRouteModule : String -> Review.Test.ReviewResult
testRouteModule routeModule =
    Review.Test.runOnModules rule
        (routeModule :: validCoreModules)


validCoreModules : List String
validCoreModules =
    [ """module Api exposing (routes)
routes = Debug.todo ""
"""
    , """module Effect exposing (routes)
routes = Debug.todo ""
"""
    , """module Shared exposing (routes)
routes = Debug.todo ""
"""
    , """module Site exposing (routes)
routes = Debug.todo ""
"""
    , """module View exposing (routes)
routes = Debug.todo ""
"""
    ]
