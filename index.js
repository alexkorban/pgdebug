#!/usr/bin/env node 

const commandLineArgs = require("command-line-args")
const commandLineUsage = require("command-line-usage")
const fs = require("fs")
const jq = require("node-jq")
const parser = require("pg-query-parser")
const readline = require("readline")
const R = require("ramda")

function buildSubqueryAsts(selectStmt, subqueries) {
    if (selectStmt.withClause !== undefined) {
        const ctes = selectStmt.withClause.WithClause.ctes
        const recursive = selectStmt.withClause.WithClause.recursive !== undefined &&
            selectStmt.withClause.WithClause.recursive
        const injectWithClause = R.evolve({SelectStmt: R.merge(R.__, {withClause: {WithClause: {ctes, recursive}}})})
        const subAsts = R.map(injectWithClause, subqueries)
        const r = R.range(1, R.length(ctes) + 1)

        const makePartialQueryAst = R.pipe(
              R.take(R.__, ctes)
            , cteSubset => {
                  const allFields = [{
                      ResTarget: {
                          val: {ColumnRef: {fields: [{"A_Star": {}}]}}
                      }
                  }]
          
                  const fromClause = [{RangeVar: {relname: R.last(cteSubset).CommonTableExpr.ctename}}]
      
                  return {
                      SelectStmt: {
                            withClause: {WithClause: {ctes: cteSubset, recursive}}
                          , targetList: allFields
                          , fromClause: fromClause
                          , op: 0
                      }
                  }
              }
        );
    
        return R.concat(R.map(makePartialQueryAst, r), subAsts)
    }
    else {
        return subqueries
    }
}

function transformQueryAst(ast, query) {
    //console.log(JSON.stringify(ast.query[0].SelectStmt, null, 2))

    //console.log("Getting subqueries from", JSON.stringify(R.pick(["fromClause"], ast.query[0].SelectStmt)))
    
    jq.run("[ .. | .subquery? | select(. != null) | select(length > 0) ]"
        , R.pick(["fromClause"], ast.query[0].SelectStmt)
        , { input: "json", output: "json" })
    .then((subqueries) => {
        const subqueryAsts = buildSubqueryAsts(ast.query[0].SelectStmt, subqueries)
        const fullQueryList = R.append(query, R.map(R.pipe(Array, parser.deparse), subqueryAsts))
        console.log(fullQueryList.join(";\n"))
    })
}

function getQuery(options) {
    if (options.command !== undefined) {
        return Promise.resolve(options.command)
    }
    else {
        return new Promise((resolve, reject) => {
            let q = ""
            const rl = readline.createInterface({
                  input: options.file !== undefined ? fs.createReadStream(options.file) : process.stdin
                , output: process.stdout
                , terminal: false
            })
    
            rl.on("line", line => q += line + "\n")
            rl.on("close", () => resolve(q))
        })
    }    
}

const cmdLineDefinitions = [
      { name: "ast", type: Boolean, description: "Output source query AST in JSON format instead of SQL statements"}
    , { name: "command", alias: "c", type: String, description: "SQL command to process" }
    , { name: "file", alias: "f", type: String, description: "File name to read SQL command from" }
    , { name: "interactive", alias: "i", type: Boolean, description: "Read SQL from stdin" }
]

const helpSections = [
  {
    header: "pgdebug",
    content: "Transforms input SQL query into a series of queries returning rows for each CTE and each subquery in the FROM clause"
  },
  {
    header: "Options",
    optionList: cmdLineDefinitions  
  }
]

// ACTION BEGINS HERE   

const options = commandLineArgs(cmdLineDefinitions)

if (R.isEmpty(options)) {
    console.log(commandLineUsage(helpSections))
    process.exit(1)
    return
}
    
getQuery(options).then(q => {
    if (R.isEmpty(R.trim(q))) {
        console.error("No query supplied")
        process.exit(1)
        return
    }
    
    const ast = parser.parse(q)
    
    if (options.ast) {
        console.log(JSON.stringify(ast, null, 2))
    }
    else {
        if (ast.error !== undefined) {
            console.error("Parsing error: ", JSON.stringify(ast.error))
            process.exit(1)
            return
        }
        else if (ast.query[0].SelectStmt === undefined) {
            console.error("Can only process SELECT statements")
            process.exit(1)
            return
        }
            
        transformQueryAst(ast, q)
    }
})
