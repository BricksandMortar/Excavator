﻿// <copyright>
// Copyright 2013 by the Spark Development Network
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Rock.Reporting
{
    /// <summary>
    /// Helper class than can extract the "Where" clause Expression from an IQueryable
    /// </summary>
    public static class FilterExpressionExtractor
    {
        /// <summary>
        /// Helps rewrite the expression by replacing the parameter expression in the qry with another parameterExpression
        /// </summary>
        private class ParameterExpressionVisitor : ExpressionVisitor
        {
            private ParameterExpression _parameterExpression;
            private string _parameterName;

            /// <summary>
            /// Initializes a new instance of the <see cref="ParameterExpressionVisitor"/> class.
            /// </summary>
            /// <param name="parameterExpression">The parameter expression.</param>
            /// <param name="parameterName">Name of the parameter.</param>
            public ParameterExpressionVisitor( Expression parameterExpression, string parameterName )
            {
                this._parameterExpression = parameterExpression as ParameterExpression;
                this._parameterName = parameterName;
            }

            /// <summary>
            /// Visits the parameter.
            /// </summary>
            /// <param name="p">The application.</param>
            /// <returns></returns>
            protected override Expression VisitParameter( ParameterExpression p )
            {
                if ( p.Name == _parameterName )
                {
                    p = _parameterExpression;
                }

                return base.VisitParameter( p );
            }
        }

        /// <summary>
        /// Extracts the "Where" clause Expression from an IQueryable
        /// </summary>
        /// <param name="qry">The qry.</param>
        /// <param name="parameterExpression">The original parameter expression.</param>
        /// <param name="parameterName">Name of the parameter (forexample: 'p') from the qry to replace with the parameterExpression.</param>
        /// <returns></returns>
        public static Expression Extract<T>( IQueryable qry, Expression parameterExpression, string parameterName )
        {
            MethodCallExpression methodCallExpression = qry.Expression as MethodCallExpression;
            Expression<Func<LambdaExpression>> executionLambda = Expression.Lambda<Func<LambdaExpression>>( methodCallExpression.Arguments[1] );
            Expression extractedExpression = ( executionLambda.Compile().Invoke() as Expression<Func<T, bool>> ).Body;

            var filterExpressionVisitor = new ParameterExpressionVisitor( parameterExpression, parameterName );

            return filterExpressionVisitor.Visit( extractedExpression );
        }
    }
}