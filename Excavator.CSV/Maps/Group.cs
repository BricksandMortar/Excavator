using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using Excavator.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Excavator.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the family import methods
    /// </summary>
    partial class CSVComponent
    {
        private const int GROUP_ID = 1;
        private const string EXCAVATOR_IMPORTED_GROUP = "ExcavatorImportedGroup";
        private const int GROUP_NAME = 2;
        private const int GROUP_CAMPUS = 15;

        /// <summary>
        /// Loads the family data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroup( CSVInstance csvData )
        {
            //TODO Assuming group ids won't clash with family ids

            // Required variables
            var lookupContext = new RockContext();
            var locationService = new LocationService( lookupContext );
            int smallGroupGroupTypeId = GroupTypeCache.Read(Rock.SystemGuid.GroupType.GROUPTYPE_SMALL_GROUP).Id;

            int meetingLocationTypeId = DefinedValueCache.Read(  Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_MEETING_LOCATION ).Id;

            var newGroupLocations = new Dictionary<GroupLocation, string>();

            var currentGroup = new Group();
            var newGroups = new List<Group>();

            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy" });
            
            int completed = 0;

            var importedSmallGroups = new GroupService( lookupContext ).Queryable().AsNoTracking()
               .Where( c => c.ForeignId != null && c.GroupTypeId == smallGroupGroupTypeId && c.ForeignKey == EXCAVATOR_IMPORTED_GROUP )
               .ToDictionary( t => ( int ) t.ForeignId, t => ( int? ) t.Id );

            ReportProgress( 0, "Starting group import" );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( (row = csvData.Database.FirstOrDefault()) != null )
            {
                int? rowGroupId = row[GROUP_ID].AsType<int?>();

                if (rowGroupId.HasValue && importedSmallGroups.ContainsKey(rowGroupId.Value))
                {
                    continue;
                }
                string rowGroupName = row[GROUP_NAME];

                if ( rowGroupId.HasValue && rowGroupId != currentGroup.ForeignId )
                {
                    currentGroup = newGroups.FirstOrDefault( g => g.ForeignId == rowGroupId );
                    if ( currentGroup == null )
                    {
                        string campusName = row[GROUP_CAMPUS];
                        currentGroup = new Group();
                        currentGroup.ForeignKey = EXCAVATOR_IMPORTED_GROUP;
                        currentGroup.ForeignId = rowGroupId;
                        currentGroup.Name = rowGroupName;
                        currentGroup.CreatedByPersonAliasId = ImportPersonAliasId;
                        currentGroup.GroupTypeId = smallGroupGroupTypeId;
                        var groupCampus = CampusList.FirstOrDefault(c => c.Name.Equals( campusName, StringComparison.InvariantCultureIgnoreCase )
                            || c.ShortCode.Equals( campusName, StringComparison.InvariantCultureIgnoreCase ));
                        if ( groupCampus == null)
                        {
                            groupCampus = new Campus();
                            groupCampus.IsSystem = false;
                            groupCampus.Name = campusName;
                            groupCampus.ShortCode = campusName.RemoveWhitespace();
                            lookupContext.Campuses.Add( groupCampus );
                            lookupContext.SaveChanges( DisableAuditing );
                            CampusList.Add( groupCampus );
                        }
                        currentGroup.CampusId = groupCampus.Id;
                        newGroups.Add( currentGroup );
                    }                   

                    // Add the family addresses since they exist in this file
                    string locationAddress = row[18];
                    string locationAddress2 = row[19];
                    string locationCity = row[20];
                    string locationState = row[21];
                    string locationZip = row[22];
                    string locationCountry = row[17];

                    var location = locationService.Get( locationAddress, locationAddress2, locationCity, locationState, locationZip, locationCountry, verifyLocation: false );

                    if ( location != null )
                    {
                        var groupMeetingLocation = new GroupLocation();
                        groupMeetingLocation.LocationId = location.Id;
                        groupMeetingLocation.IsMailingLocation = false;
                        groupMeetingLocation.IsMappedLocation = true;
                        groupMeetingLocation.GroupLocationTypeValueId = meetingLocationTypeId;
                        // TODO Schedules
                        groupMeetingLocation.Schedules.Add();
                        newGroupLocations.Add( groupMeetingLocation, rowGroupId.ToString() );
                    }
                    
                    currentGroup.CreatedDateTime = ImportDateTime;
                    currentGroup.ModifiedDateTime = ImportDateTime;

                    completed++;
                    if ( completed % (ReportingNumber * 10) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} families imported.", completed ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveFamilies( newGroups, newGroupLocations );
                        ReportPartialProgress();

                        // Reset lookup context
                        lookupContext.SaveChanges();
                        lookupContext = new RockContext();
                        locationService = new LocationService( lookupContext );
                        newGroups.Clear();
                        newGroupLocations.Clear();
                    }
                }
            }

            // Check to see if any rows didn't get saved to the database
            if ( newGroupLocations.Any() )
            {
                SaveFamilies( newGroups, newGroupLocations );
            }

            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished family import: {0:N0} families added or updated.", completed ) );
            return completed;
        }

        /// <summary>
        /// Saves all family changes.
        /// </summary>
        private void SaveFamilies( List<Group> newFamilyList, Dictionary<GroupLocation, string> newGroupLocations )
        {
            var rockContext = new RockContext();

            // First save any unsaved families
            if ( newFamilyList.Any() )
            {
                rockContext.WrapTransaction( ( ) =>
                {
                    rockContext.Groups.AddRange( newFamilyList );
                    rockContext.SaveChanges( DisableAuditing );
                } );

                // Add these new families to the global list
                ImportedFamilies.AddRange( newFamilyList );
            }

            // Now save locations
            if ( newGroupLocations.Any() )
            {
                // Set updated family id on locations
                foreach ( var locationPair in newGroupLocations )
                {
                    int? familyGroupId = ImportedFamilies.Where( g => g.ForeignKey == locationPair.Value ).Select( g => ( int? )g.Id ).FirstOrDefault();
                    if ( familyGroupId != null )
                    {
                        locationPair.Key.GroupId = ( int )familyGroupId;
                    }
                }

                // Save locations
                rockContext.WrapTransaction( ( ) =>
                {
                    rockContext.Configuration.AutoDetectChangesEnabled = false;
                    rockContext.GroupLocations.AddRange( newGroupLocations.Keys );
                    rockContext.ChangeTracker.DetectChanges();
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }
    }
}