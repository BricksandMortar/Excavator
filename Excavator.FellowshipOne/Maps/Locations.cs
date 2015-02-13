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
using Rock.Web.Cache;

namespace Excavator.F1
{
    partial class F1Component
    {
        /// <summary>
        /// Maps the activity ministry.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <returns></returns>
        private void MapActivityMinistry( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();

            // Add an Attribute for the unique F1 Ministry Id
            int groupEntityTypeId = EntityTypeCache.Read( "Rock.Model.Group" ).Id;
            var ministryAttributeId = new AttributeService( lookupContext ).Queryable().Where( a => a.EntityTypeId == groupEntityTypeId
                && a.Key == "F1MinistryId" ).Select( a => a.Id ).FirstOrDefault();
            if ( ministryAttributeId == 0 )
            {
                var newMinistryAttribute = new Rock.Model.Attribute();
                newMinistryAttribute.Key = "F1MinistryId";
                newMinistryAttribute.Name = "F1 Ministry Id";
                newMinistryAttribute.FieldTypeId = IntegerFieldTypeId;
                newMinistryAttribute.EntityTypeId = groupEntityTypeId;
                newMinistryAttribute.EntityTypeQualifierValue = string.Empty;
                newMinistryAttribute.EntityTypeQualifierColumn = string.Empty;
                newMinistryAttribute.Description = "The FellowshipOne identifier for the ministry that was imported";
                newMinistryAttribute.DefaultValue = string.Empty;
                newMinistryAttribute.IsMultiValue = false;
                newMinistryAttribute.IsRequired = false;
                newMinistryAttribute.Order = 0;

                lookupContext.Attributes.Add( newMinistryAttribute );
                lookupContext.SaveChanges( DisableAudit );
                ministryAttributeId = newMinistryAttribute.Id;
            }

            // Get previously imported Ministries
            var importedMinistries = new AttributeValueService( lookupContext ).GetByAttributeId( ministryAttributeId )
                .ToDictionary( t => t.Value.AsType<int?>(), t => t.EntityId );

            var newGroups = new List<Group>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying ministry import ({0:N0} found).", totalRows ) );

            foreach ( var row in tableData )
            {
                int? ministryId = row["Ministry_ID"] as int?;
                if ( ministryId != null && !importedMinistries.ContainsKey( ministryId ) )
                {
                    string ministryName = row["Ministry_Name"] as string;
                    bool? ministryIsActive = row["Ministry_Active"] as bool?;

                    int? activityId = row["Activity_ID"] as int?;
                    string activityName = row["Activity_Name"] as string;
                    bool? activityIsActive = row["Activity_Active"] as bool?;

                    if ( ministryName != null )
                    {
                        var ministry = new Group();
                        ministry.Name = ministryName.Trim();
                        ministry.IsActive = ministryIsActive ?? false;
                        ministry.CampusId = CampusList.Where( c => ministryName.StartsWith( c.Name ) || ministryName.StartsWith( c.ShortCode ) )
                            .Select( c => (int?)c.Id ).FirstOrDefault();

                        // create new group for activity with ministry as parent group

                        newGroups.Add( ministry );
                        completed++;

                        if ( completed % percentage < 1 )
                        {
                            int percentComplete = completed / percentage;
                            ReportProgress( percentComplete, string.Format( "{0:N0} ministries imported ({1}% complete).", completed, percentComplete ) );
                        }
                        else if ( completed % ReportingNumber < 1 )
                        {
                            SaveActivityMinistry( newGroups );
                            ReportPartialProgress();
                            newGroups.Clear();
                        }
                    }
                }
            }

            if ( newGroups.Any() )
            {
                SaveActivityMinistry( newGroups );
            }

            ReportProgress( 100, string.Format( "Finished ministry import: {0:N0} ministries imported.", completed ) );
        }

        /// <summary>
        /// Saves the ministries.
        /// </summary>
        /// <param name="newGroups">The new groups.</param>
        private static void SaveActivityMinistry( List<Group> newGroups )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.Groups.AddRange( newGroups );
                rockContext.SaveChanges( DisableAudit );
            } );
        }

        /// <summary>
        /// Maps the RLC data to rooms, locations & classes
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <returns></returns>
        private void MapRLC( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var attributeValueService = new AttributeValueService( lookupContext );
            var attributeService = new AttributeService( lookupContext );

            int groupEntityTypeId = EntityTypeCache.Read( "Rock.Model.Group" ).Id;

            // Get any previously imported RLCs
            foreach ( var row in tableData )
            {
                int? rlcId = row["RLC_ID"] as int?;
                if ( rlcId != null )
                {
                    // Activity_ID
                    // RLC_Name
                    // Activity_Group_ID
                    // Start_Age_Date
                    // End_Age_Date
                    // Is_Active
                    // Room_Code
                    // Room_Desc
                    // Room_Name
                    // Max_Capacity
                    // Building_Name

                    // set location.ForeignId to RLC_id
                    // set group.ForeignId to Activity_id
                }
            }
        }

        /// <summary>
        /// Maps the family address.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <returns></returns>
        private void MapFamilyAddress( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var locationService = new LocationService( lookupContext );

            List<GroupMember> groupMembershipList = new GroupMemberService( lookupContext ).Queryable().Where( gm => gm.Group.GroupType.Guid == new Guid( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY ) ).ToList();

            var groupLocationTypeList = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE ), lookupContext ).DefinedValues;
            int homeGroupLocationTypeId = groupLocationTypeList.FirstOrDefault( dv => dv.Guid == new Guid( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_HOME ) ).Id;
            int workGroupLocationTypeId = groupLocationTypeList.FirstOrDefault( dv => dv.Guid == new Guid( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_WORK ) ).Id;
            int previousGroupLocationTypeId = groupLocationTypeList.FirstOrDefault( dv => dv.Guid == new Guid( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_PREVIOUS ) ).Id;

            var newGroupLocations = new List<GroupLocation>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying address import ({0:N0} found).", totalRows ) );

            foreach ( var row in tableData )
            {
                int? individualId = row["Individual_ID"] as int?;
                int? householdId = row["Household_ID"] as int?;
                int? associatedPersonId = GetPersonAliasId( individualId, householdId );
                if ( associatedPersonId != null )
                {
                    var familyGroup = groupMembershipList.Where( gm => gm.PersonId == (int)associatedPersonId )
                        .Select( gm => gm.Group ).FirstOrDefault();

                    if ( familyGroup != null )
                    {
                        var groupLocation = new GroupLocation();

                        string street1 = row["Address_1"] as string;
                        string street2 = row["Address_2"] as string;
                        string city = row["City"] as string;
                        string state = row["State"] as string;
                        string country = row["country"] as string; // NOT A TYPO: F1 has property in lower-case
                        string zip = row["Postal_Code"] as string;

                        Location familyAddress = locationService.Get( street1, street2, city, state, zip, country );

                        if ( familyAddress != null )
                        {
                            familyAddress.CreatedByPersonAliasId = ImportPersonAlias.Id;
                            familyAddress.Name = familyGroup.Name;
                            familyAddress.IsActive = true;

                            groupLocation.GroupId = familyGroup.Id;
                            groupLocation.LocationId = familyAddress.Id;
                            groupLocation.IsMailingLocation = true;
                            groupLocation.IsMappedLocation = true;

                            string addressType = row["Address_Type"].ToString().ToLower();
                            if ( addressType.Equals( "primary" ) )
                            {
                                groupLocation.GroupLocationTypeValueId = homeGroupLocationTypeId;
                            }
                            else if ( addressType.Equals( "business" ) || addressType.ToLower().Equals( "org" ) )
                            {
                                groupLocation.GroupLocationTypeValueId = workGroupLocationTypeId;
                            }
                            else if ( addressType.Equals( "previous" ) )
                            {
                                groupLocation.GroupLocationTypeValueId = previousGroupLocationTypeId;
                            }
                            else if ( !string.IsNullOrEmpty( addressType ) )
                            {
                                var customTypeId = groupLocationTypeList.Where( dv => dv.Value.ToLower().Equals( addressType ) )
                                    .Select( dv => (int?)dv.Id ).FirstOrDefault();
                                groupLocation.GroupLocationTypeValueId = customTypeId ?? homeGroupLocationTypeId;
                            }

                            newGroupLocations.Add( groupLocation );
                            completed++;

                            if ( completed % percentage < 1 )
                            {
                                int percentComplete = completed / percentage;
                                ReportProgress( percentComplete, string.Format( "{0:N0} addresses imported ({1}% complete).", completed, percentComplete ) );
                            }
                            else if ( completed % ReportingNumber < 1 )
                            {
                                SaveFamilyAddress( newGroupLocations );

                                // Reset context
                                newGroupLocations.Clear();
                                lookupContext = new RockContext();
                                locationService = new LocationService( lookupContext );

                                ReportPartialProgress();
                            }
                        }
                    }
                }
            }

            if ( newGroupLocations.Any() )
            {
                SaveFamilyAddress( newGroupLocations );
            }

            ReportProgress( 100, string.Format( "Finished address import: {0:N0} addresses imported.", completed ) );
        }

        /// <summary>
        /// Saves the family address.
        /// </summary>
        /// <param name="newGroupLocations">The new group locations.</param>
        private static void SaveFamilyAddress( List<GroupLocation> newGroupLocations )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.GroupLocations.AddRange( newGroupLocations );
                rockContext.SaveChanges( DisableAudit );
            } );
        }
    }
}