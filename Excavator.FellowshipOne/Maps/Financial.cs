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
using Excavator.Utility;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Security;
using Rock.Web.Cache;

namespace Excavator.F1
{
    /// <summary>
    /// Partial of F1Component that holds the Financial import methods
    /// </summary>
    partial class F1Component
    {
        /// <summary>
        /// Maps the account data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <returns></returns>
        private void MapBankAccount( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var importedBankAccounts = new FinancialPersonBankAccountService( lookupContext ).Queryable().ToList();
            var newBankAccounts = new List<FinancialPersonBankAccount>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying check number import ({0:N0} found, {1:N0} already exist).", totalRows, importedBankAccounts.Count() ) );

            foreach ( var row in tableData )
            {
                int? individualId = row["Individual_ID"] as int?;
                int? householdId = row["Household_ID"] as int?;
                int? personId = GetPersonAliasId( individualId, householdId );
                if ( personId != null )
                {
                    int? routingNumber = row["Routing_Number"] as int?;
                    string accountNumber = row["Account"] as string;
                    if ( routingNumber != null && !string.IsNullOrWhiteSpace( accountNumber ) )
                    {
                        accountNumber = accountNumber.Replace( " ", string.Empty );
                        string encodedNumber = FinancialPersonBankAccount.EncodeAccountNumber( routingNumber.ToString(), accountNumber );
                        if ( !importedBankAccounts.Any( a => a.PersonAliasId == personId && a.AccountNumberSecured == encodedNumber ) )
                        {
                            var bankAccount = new FinancialPersonBankAccount();
                            bankAccount.CreatedByPersonAliasId = ImportPersonAlias.Id;
                            bankAccount.AccountNumberSecured = encodedNumber;
                            bankAccount.AccountNumberMasked = accountNumber.ToString().Masked();
                            bankAccount.PersonAliasId = (int)personId;

                            // Other Attributes (not used):
                            // Account_Type_Name

                            newBankAccounts.Add( bankAccount );
                            completed++;
                            if ( completed % percentage < 1 )
                            {
                                int percentComplete = completed / percentage;
                                ReportProgress( percentComplete, string.Format( "{0:N0} numbers imported ({1}% complete).", completed, percentComplete ) );
                            }
                            else if ( completed % ReportingNumber < 1 )
                            {
                                SaveBankAccounts( newBankAccounts );
                                newBankAccounts.Clear();
                                ReportPartialProgress();
                            }
                        }
                    }
                }
            }

            if ( newBankAccounts.Any() )
            {
                SaveBankAccounts( newBankAccounts );
            }

            ReportProgress( 100, string.Format( "Finished check number import: {0:N0} numbers imported.", completed ) );
        }

        /// <summary>
        /// Saves the bank accounts.
        /// </summary>
        /// <param name="newBankAccounts">The new bank accounts.</param>
        private static void SaveBankAccounts( List<FinancialPersonBankAccount> newBankAccounts )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialPersonBankAccounts.AddRange( newBankAccounts );
                rockContext.SaveChanges( DisableAudit );
            } );
        }

        /// <summary>
        /// Maps the batch data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private void MapBatch( IQueryable<Row> tableData )
        {
            var batchStatusClosed = Rock.Model.BatchStatus.Closed;
            var newBatches = new List<FinancialBatch>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying batch import ({0:N0} found, {1:N0} already exist).", totalRows, ImportedBatches.Count() ) );
            foreach ( var row in tableData )
            {
                int? batchId = row["BatchID"] as int?;
                if ( batchId != null && !ImportedBatches.ContainsKey( (int)batchId ) )
                {
                    var batch = new FinancialBatch();
                    batch.CreatedByPersonAliasId = ImportPersonAlias.Id;
                    batch.ForeignId = batchId.ToString();
                    batch.Status = batchStatusClosed;

                    string name = row["BatchName"] as string;
                    if ( name != null )
                    {
                        name = name.Trim();
                        batch.Name = name.Left( 50 );
                        batch.CampusId = CampusList.Where( c => name.StartsWith( c.Name ) || name.StartsWith( c.ShortCode ) )
                            .Select( c => (int?)c.Id ).FirstOrDefault();
                    }

                    DateTime? batchDate = row["BatchDate"] as DateTime?;
                    if ( batchDate != null )
                    {
                        batch.BatchStartDateTime = batchDate;
                        batch.BatchEndDateTime = batchDate;
                    }

                    decimal? amount = row["BatchAmount"] as decimal?;
                    if ( amount != null )
                    {
                        batch.ControlAmount = amount.HasValue ? amount.Value : new decimal();
                    }

                    newBatches.Add( batch );
                    completed++;
                    if ( completed % percentage < 1 )
                    {
                        int percentComplete = completed / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} batches imported ({1}% complete).", completed, percentComplete ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveFinancialBatches( newBatches );
                        newBatches.ForEach( b => ImportedBatches.Add( b.ForeignId.AsType<int>(), (int?)b.Id ) );
                        newBatches.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            if ( newBatches.Any() )
            {
                SaveFinancialBatches( newBatches );
                newBatches.ForEach( b => ImportedBatches.Add( b.ForeignId.AsType<int>(), (int?)b.Id ) );
            }

            ReportProgress( 100, string.Format( "Finished batch import: {0:N0} batches imported.", completed ) );
        }

        /// <summary>
        /// Saves the financial batches.
        /// </summary>
        /// <param name="newBatches">The new batches.</param>
        private static void SaveFinancialBatches( List<FinancialBatch> newBatches )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialBatches.AddRange( newBatches );
                rockContext.SaveChanges( DisableAudit );
            } );
        }

        /// <summary>
        /// Maps the contribution.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="selectedColumns">The selected columns.</param>
        private void MapContribution( IQueryable<Row> tableData, List<string> selectedColumns = null )
        {
            var lookupContext = new RockContext();
            int transactionEntityTypeId = EntityTypeCache.Read( "Rock.Model.FinancialTransaction" ).Id;
            var transactionTypeContributionId = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.TRANSACTION_TYPE_CONTRIBUTION ), lookupContext ).Id;

            int currencyTypeACH = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_ACH ), lookupContext ).Id;
            int currencyTypeCash = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CASH ), lookupContext ).Id;
            int currencyTypeCheck = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CHECK ), lookupContext ).Id;
            int currencyTypeCreditCard = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CREDIT_CARD ), lookupContext ).Id;

            var refundReasons = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_REFUND_REASON ), lookupContext ).DefinedValues;

            List<FinancialPledge> pledgeList = new FinancialPledgeService( lookupContext ).Queryable().ToList();
            List<FinancialAccount> accountList = new FinancialAccountService( lookupContext ).Queryable().ToList();

            // Get all imported contributions
            var importedContributions = new FinancialTransactionService( lookupContext ).Queryable()
               .Where( c => c.ForeignId != null )
               .ToDictionary( t => t.ForeignId.AsType<int>(), t => (int?)t.Id );

            // List for batching new contributions
            var newTransactions = new List<FinancialTransaction>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying contribution import ({0:N0} found, {1:N0} already exist).", totalRows, importedContributions.Count() ) );
            foreach ( var row in tableData )
            {
                int? individualId = row["Individual_ID"] as int?;
                int? householdId = row["Household_ID"] as int?;
                int? contributionId = row["ContributionID"] as int?;

                if ( contributionId != null && !importedContributions.ContainsKey( (int)contributionId ) )
                {
                    var transaction = new FinancialTransaction();
                    transaction.TransactionTypeValueId = transactionTypeContributionId;
                    transaction.AuthorizedPersonAliasId = GetPersonAliasId( individualId, householdId );
                    transaction.CreatedByPersonAliasId = ImportPersonAlias.Id;
                    transaction.ProcessedByPersonAliasId = GetPersonAliasId( individualId, householdId );
                    transaction.ForeignId = contributionId.ToStringSafe();

                    string summary = row["Memo"] as string;
                    if ( summary != null )
                    {
                        transaction.Summary = summary;
                    }

                    int? batchId = row["BatchID"] as int?;
                    if ( batchId != null && ImportedBatches.Any( b => b.Key == batchId ) )
                    {
                        transaction.BatchId = ImportedBatches.FirstOrDefault( b => b.Key == batchId ).Value;
                    }

                    DateTime? receivedDate = row["Received_Date"] as DateTime?;
                    if ( receivedDate != null )
                    {
                        transaction.TransactionDateTime = receivedDate;
                        transaction.CreatedDateTime = receivedDate;
                    }

                    bool isTypeNonCash = false;
                    string contributionType = row["Contribution_Type_Name"].ToString().ToLower();
                    if ( contributionType != null )
                    {
                        if ( contributionType == "ach" )
                        {
                            transaction.CurrencyTypeValueId = currencyTypeACH;
                        }
                        else if ( contributionType == "cash" )
                        {
                            transaction.CurrencyTypeValueId = currencyTypeCash;
                        }
                        else if ( contributionType == "check" )
                        {
                            transaction.CurrencyTypeValueId = currencyTypeCheck;
                        }
                        else if ( contributionType == "credit card" )
                        {
                            transaction.CurrencyTypeValueId = currencyTypeCreditCard;
                        }
                        else
                        {
                            isTypeNonCash = true;
                        }
                    }

                    string checkNumber = row["Check_Number"] as string;
                    if ( checkNumber != null && checkNumber.AsType<int?>() != null )
                    {
                        // set the transaction code to the check number
                        transaction.TransactionCode = checkNumber;
                    }

                    string fundName = row["Fund_Name"] as string;
                    decimal? amount = row["Amount"] as decimal?;
                    if ( fundName != null & amount != null )
                    {
                        FinancialAccount matchingAccount = null;
                        int? parentAccountId = null;
                        int? fundCampusId = null;
                        fundName = fundName.Trim();

                        string subFund = row["Sub_Fund_Name"] as string;
                        if ( subFund != null )
                        {
                            subFund = subFund.Trim();

                            // Check if subfund was used to mark a multi-site campus
                            fundCampusId = CampusList.Where( c => subFund.StartsWith( c.Name ) || subFund.StartsWith( c.ShortCode ) )
                                .Select( c => (int?)c.Id ).FirstOrDefault();

                            // Matched a campus, check to see if an account exists for that campus already
                            if ( fundCampusId != null )
                            {
                                matchingAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName )
                                    && a.CampusId != null && a.CampusId.Equals( fundCampusId ) );
                            }
                            else
                            {
                                // No campus match, look for an account that matches parent name and subfund name
                                matchingAccount = accountList.FirstOrDefault( a => a.ParentAccountId != null && a.ParentAccount.Name.Equals( fundName ) && a.Name.Equals( subFund ) );

                                if ( matchingAccount == null )
                                {
                                    // Check if a parent account exists already
                                    FinancialAccount parentAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName ) );
                                    if ( parentAccount == null )
                                    {
                                        parentAccount = AddAccount( lookupContext, fundName, fundCampusId, null );
                                        accountList.Add( parentAccount );
                                    }

                                    // set data for subfund to be created
                                    parentAccountId = parentAccount.Id;
                                    fundName = subFund;
                                }
                            }
                        }
                        else
                        {
                            matchingAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName ) && a.CampusId == null );
                        }

                        if ( matchingAccount == null )
                        {
                            // No account matches, create the new account with campus Id and parent Id if they were set
                            matchingAccount = AddAccount( lookupContext, fundName, fundCampusId, parentAccountId );
                            accountList.Add( matchingAccount );
                        }

                        var transactionDetail = new FinancialTransactionDetail();
                        transactionDetail.Amount = (decimal)amount;
                        transactionDetail.CreatedDateTime = receivedDate;
                        transactionDetail.AccountId = matchingAccount.Id;
                        transactionDetail.IsNonCash = isTypeNonCash;
                        transaction.TransactionDetails.Add( transactionDetail );

                        if ( amount < 0 )
                        {
                            var transactionRefund = new FinancialTransactionRefund();
                            transactionRefund.CreatedDateTime = receivedDate;
                            transactionRefund.RefundReasonSummary = summary;
                            transactionRefund.RefundReasonValueId = refundReasons.Where( dv => summary != null && dv.Value.Contains( summary ) )
                                .Select( dv => (int?)dv.Id ).FirstOrDefault();
                            transaction.Refund = transactionRefund;
                        }
                    }

                    newTransactions.Add( transaction );
                    completed++;
                    if ( completed % percentage < 1 )
                    {
                        int percentComplete = completed / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} contributions imported ({1}% complete).", completed, percentComplete ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveContributions( newTransactions );
                        newTransactions.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            if ( newTransactions.Any() )
            {
                SaveContributions( newTransactions );
            }

            ReportProgress( 100, string.Format( "Finished contribution import: {0:N0} contributions imported.", completed ) );
        }

        /// <summary>
        /// Saves the contributions.
        /// </summary>
        /// <param name="newTransactions">The new transactions.</param>
        private static void SaveContributions( List<FinancialTransaction> newTransactions )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialTransactions.AddRange( newTransactions );
                rockContext.SaveChanges( DisableAudit );
            } );
        }

        /// <summary>
        /// Maps the pledge.
        /// </summary>
        /// <param name="queryable">The queryable.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private void MapPledge( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            List<FinancialAccount> accountList = new FinancialAccountService( lookupContext ).Queryable().ToList();
            List<FinancialPledge> importedPledges = new FinancialPledgeService( lookupContext ).Queryable().ToList();

            var pledgeFrequencies = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_FREQUENCY ), lookupContext ).DefinedValues;

            var newPledges = new List<FinancialPledge>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying pledge import ({0:N0} found).", totalRows ) );

            foreach ( var row in tableData )
            {
                decimal? amount = row["Total_Pledge"] as decimal?;
                DateTime? startDate = row["Start_Date"] as DateTime?;
                DateTime? endDate = row["End_Date"] as DateTime?;
                if ( amount != null && startDate != null && endDate != null )
                {
                    int? individualId = row["Individual_ID"] as int?;
                    int? householdId = row["Household_ID"] as int?;
                    int? personId = GetPersonAliasId( individualId, householdId );
                    if ( personId != null && !importedPledges.Any( p => p.PersonAliasId == personId && p.TotalAmount == amount && p.StartDate.Equals( startDate ) ) )
                    {
                        var pledge = new FinancialPledge();
                        pledge.CreatedByPersonAliasId = ImportPersonAlias.Id;
                        pledge.StartDate = (DateTime)startDate;
                        pledge.EndDate = (DateTime)endDate;
                        pledge.TotalAmount = (decimal)amount;

                        string frequency = row["Pledge_Frequency_Name"].ToString().ToLower();
                        if ( frequency != null )
                        {
                            if ( frequency == "one time" || frequency == "as can" )
                            {
                                pledge.PledgeFrequencyValueId = pledgeFrequencies.FirstOrDefault( f => f.Guid == new Guid( Rock.SystemGuid.DefinedValue.TRANSACTION_FREQUENCY_ONE_TIME ) ).Id;
                            }
                            else
                            {
                                pledge.PledgeFrequencyValueId = pledgeFrequencies
                                    .Where( f => f.Value.ToLower().StartsWith( frequency ) || f.Description.ToLower().StartsWith( frequency ) )
                                    .Select( f => f.Id ).FirstOrDefault();
                            }
                        }

                        string fundName = row["Fund_Name"] as string;
                        string subFund = row["Sub_Fund_Name"] as string;
                        if ( fundName != null )
                        {
                            FinancialAccount matchingAccount = null;
                            int? fundCampusId = null;
                            if ( subFund != null )
                            {
                                // match by campus if the subfund appears to be a campus
                                fundCampusId = CampusList.Where( c => c.Name.StartsWith( subFund ) || c.ShortCode == subFund )
                                    .Select( c => (int?)c.Id ).FirstOrDefault();

                                if ( fundCampusId != null )
                                {
                                    matchingAccount = accountList.FirstOrDefault( a => a.Name.StartsWith( fundName ) && a.CampusId != null && a.CampusId.Equals( fundCampusId ) );
                                }
                                else
                                {
                                    matchingAccount = accountList.FirstOrDefault( a => a.Name.StartsWith( fundName ) && a.Name.StartsWith( subFund ) );
                                }
                            }
                            else
                            {
                                matchingAccount = accountList.FirstOrDefault( a => a.Name.StartsWith( fundName ) );
                            }

                            if ( matchingAccount == null )
                            {
                                matchingAccount = AddAccount( lookupContext, fundName, fundCampusId, null );
                                accountList.Add( matchingAccount );
                            }

                            pledge.AccountId = matchingAccount.Id;
                        }

                        // Attributes to add?
                        // Pledge_Drive_Name

                        newPledges.Add( pledge );
                        completed++;
                        if ( completed % percentage < 1 )
                        {
                            int percentComplete = completed / percentage;
                            ReportProgress( percentComplete, string.Format( "{0:N0} pledges imported ({1}% complete).", completed, percentComplete ) );
                        }
                        else if ( completed % ReportingNumber < 1 )
                        {
                            SavePledges( newPledges );
                            ReportPartialProgress();
                        }
                    }
                }
            }

            if ( newPledges.Any() )
            {
                SavePledges( newPledges );
            }

            ReportProgress( 100, string.Format( "Finished pledge import: {0:N0} pledges imported.", completed ) );
        }

        /// <summary>
        /// Saves the pledges.
        /// </summary>
        /// <param name="newPledges">The new pledges.</param>
        private static void SavePledges( List<FinancialPledge> newPledges )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialPledges.AddRange( newPledges );
                rockContext.SaveChanges( DisableAudit );
            } );
        }

        /// <summary>
        /// Adds the account.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="fundName">Name of the fund.</param>
        /// <param name="fundCampusId">The fund campus identifier.</param>
        /// <returns></returns>
        private FinancialAccount AddAccount( RockContext lookupContext, string fundName, int? fundCampusId, int? parentAccountId )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }

            var account = new FinancialAccount();
            account.Name = fundName;
            account.PublicName = fundName;
            account.IsTaxDeductible = true;
            account.IsActive = true;
            account.CampusId = fundCampusId;
            account.ParentAccountId = parentAccountId;
            account.CreatedByPersonAliasId = ImportPersonAlias.Id;

            lookupContext.FinancialAccounts.Add( account );
            lookupContext.SaveChanges( DisableAudit );

            return account;
        }
    }
}