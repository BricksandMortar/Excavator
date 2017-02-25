using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using Excavator.Utility;
using Ical.Net;
using Ical.Net.DataTypes;
using Ical.Net.Interfaces.DataTypes;
using Ical.Net.Serialization;
using Ical.Net.Serialization.iCalendar.Serializers;
using NodaTime;
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
        private int MapGroup( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var locationService = new LocationService( lookupContext );
            
            int smallGroupGroupTypeId = GroupTypeCache.Read(Rock.SystemGuid.GroupType.GROUPTYPE_SMALL_GROUP).Id;

            int meetingLocationTypeId = DefinedValueCache.Read(  Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_MEETING_LOCATION ).Id;

            var newGroupLocations = new Dictionary<int, GroupLocation>();

            var groupEntityTypeId = EntityTypeCache.Read("Rock.Model.Group").Id;
            var smallGroupAttributes = new AttributeService( lookupContext ).GetByEntityTypeId( groupEntityTypeId ).ToList();

            var currentGroup = new Group();
            var newGroups = new List<Group>();

            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > 29 )
                .ToDictionary( f => f.index, f => f.node.Name.RemoveWhitespace() );

            // Add any attributes if they don't already exist
            if ( customAttributes.Any() )
            {
                var newAttributes = new List<Rock.Model.Attribute>();
                foreach ( var newAttributePair in customAttributes.Where( ca => !smallGroupAttributes.Any( a => a.Key == ca.Value ) ) )
                {
                    var newAttribute = new Rock.Model.Attribute();
                    newAttribute.Name = newAttributePair.Value;
                    newAttribute.Key = newAttributePair.Value.RemoveWhitespace();
                    newAttribute.Description = newAttributePair.Value + " created by CSV import";
                    newAttribute.EntityTypeQualifierValue = smallGroupGroupTypeId.ToString();
                    newAttribute.EntityTypeQualifierColumn = "GroupTypeId";
                    newAttribute.EntityTypeId = groupEntityTypeId;
                    newAttribute.FieldTypeId = FieldTypeCache.Read( new Guid( Rock.SystemGuid.FieldType.TEXT ), lookupContext ).Id;
                    newAttribute.DefaultValue = string.Empty;
                    newAttribute.IsMultiValue = false;
                    newAttribute.IsGridColumn = false;
                    newAttribute.IsRequired = false;
                    newAttribute.Order = 0;
                    newAttributes.Add( newAttribute );
                }

                lookupContext.Attributes.AddRange( newAttributes );
                lookupContext.SaveChanges( DisableAuditing );
                smallGroupAttributes.AddRange( newAttributes );
            }

            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy", "M/dd/yyyy", "M/d/yyyy" };
            
            int completed = 0;

            // dictionary: F1Id, Group
            ImportedSmallGroups = new GroupService( lookupContext ).Queryable().AsNoTracking()
               .Where( c => c.ForeignId != null && c.GroupTypeId == smallGroupGroupTypeId && c.ForeignKey == EXCAVATOR_IMPORTED_GROUP )
                                                                   // ReSharper disable once PossibleInvalidOperationException
               .ToDictionary( t => ( int ) t.ForeignId, t => t );

            ReportProgress( 0, "Starting group import" );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( (row = csvData.Database.FirstOrDefault()) != null )
            {
                int? rowGroupId = row[GROUP_ID].AsType<int?>();

                if (rowGroupId.HasValue && ImportedSmallGroups.ContainsKey(rowGroupId.Value))
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
                        currentGroup.Description = row[3];
                        currentGroup.CreatedByPersonAliasId = ImportPersonAliasId;
                        currentGroup.GroupTypeId = smallGroupGroupTypeId;
                        if (!string.IsNullOrEmpty(campusName))
                        {
                            var groupCampus = CampusList.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.InvariantCultureIgnoreCase )
                             || c.ShortCode.Equals( campusName, StringComparison.InvariantCultureIgnoreCase ) );
                            if ( groupCampus == null )
                            {
                                groupCampus = new Campus();
                                groupCampus.IsActive = true;
                                groupCampus.IsSystem = false;
                                groupCampus.Name = campusName;
                                groupCampus.ShortCode = campusName.RemoveWhitespace();
                                lookupContext.Campuses.Add( groupCampus );
                                lookupContext.SaveChanges( DisableAuditing );
                                CampusList.Add( groupCampus );
                            }
                            currentGroup.CampusId = groupCampus.Id;
                        }
                        newGroups.Add( currentGroup );
                    }                   

                    // Add the address since they exist in this file
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

                        int frequency = 0;
                        var frequencyType = FrequencyType.None;
                        if (!string.IsNullOrEmpty(row[24]))
                        {
                             frequency = GetFrequency( row[24], out frequencyType );
                        }

                        Schedule schedule = null;
                        if (frequencyType == FrequencyType.Weekly)
                        {
                            schedule = CreateWeeklySchedule( ( DayOfWeek ) Enum.Parse( typeof( DayOfWeek ), row[25] ), DateTime.ParseExact( row[26], dateFormats, new CultureInfo( "en-US" ), DateTimeStyles.None ), new LocalTime(), new LocalTime(), frequency );
                        }
                        else if (frequencyType == FrequencyType.Monthly)
                        {
                            schedule = CreateMonthlySchedule(DateTime.ParseExact(row[26], dateFormats, new CultureInfo("en-US"), DateTimeStyles.None), new LocalTime(), new LocalTime());
                        }

                        if (schedule != null)
                        {
                            groupMeetingLocation.Schedules.Add( schedule );
                            currentGroup.Schedule = schedule;
                        }
                        
                        newGroupLocations.Add( rowGroupId.Value, groupMeetingLocation );
                    }
                    
                    currentGroup.CreatedDateTime = ImportDateTime;
                    currentGroup.ModifiedDateTime = ImportDateTime;
                    currentGroup.Attributes = new Dictionary<string, AttributeCache>();
                    currentGroup.AttributeValues = new Dictionary<string, AttributeValueCache>();

                    foreach ( var attributePair in customAttributes )
                    {
                        string newAttributeValue = row[attributePair.Key];
                        if ( !string.IsNullOrWhiteSpace( newAttributeValue ) )
                        {
                            // check if this attribute value is a date
                            DateTime valueAsDateTime;
                            if ( DateTime.TryParseExact( newAttributeValue, dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out valueAsDateTime ) )
                            {
                                newAttributeValue = valueAsDateTime.ToString( "yyyy-MM-dd" );
                            }

                            int? newAttributeId = smallGroupAttributes.Where( a => a.Key == attributePair.Value.RemoveWhitespace() )
                                .Select( a => ( int? ) a.Id ).FirstOrDefault();
                            if ( newAttributeId != null )
                            {
                                var newAttribute = AttributeCache.Read( ( int ) newAttributeId );
                                AddGroupAttribute( newAttribute, currentGroup, newAttributeValue );
                            }
                        }
                    }


                    completed++;
                    if ( completed % (ReportingNumber * 10) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} groups imported.", completed ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveGroups( newGroups, newGroupLocations );
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
                SaveGroups( newGroups, newGroupLocations );
            }

            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished group import: {0:N0} groups added or updated.", completed ) );
            return completed;
        }

        private static int GetFrequency(string input, out FrequencyType frequencyType)
        {
            if (input.Length == 1 && input.AsIntegerOrNull() != null)
            {
                frequencyType = FrequencyType.Monthly;
                return input.AsInteger();
            }
            frequencyType = FrequencyType.Weekly;

            switch (input)
            {
                case "Every 2 Weeks":
                    return 2;
                case "Every Week":
                    return 1;
                default:
                    throw new Exception( "Failed to parse frequency of group schedule" );
            }

        }

        private Schedule CreateWeeklySchedule(DayOfWeek dayOfWeek, DateTime startDate, LocalTime startTime, LocalTime endTime, int weekFrequency )
        {
            var schedule = new Schedule();
            schedule.WeeklyDayOfWeek = dayOfWeek;
            schedule.EffectiveStartDate = startDate;
            schedule.WeeklyTimeOfDay = new TimeSpan(startTime.Hour, startDate.Minute, startDate.Second);

            var endOfSession = new DateTime(startDate.Year, startDate.Month, startDate.Day, endTime.Hour, endTime.Minute, endTime.Second);
            var recurrenceRule = new RecurrencePattern(FrequencyType.Weekly, weekFrequency);

            var e = new Event()
            {
                DtStart = new CalDateTime(startDate.AddHours(startTime.Hour).AddMinutes(startDate.Minute)),
                DtEnd = new CalDateTime(endOfSession),
                RecurrenceRules = new List<IRecurrencePattern> {recurrenceRule}
            };

            var calendar = new Ical.Net.Calendar();
            calendar.Events.Add(e);

            var serializer = new CalendarSerializer(new SerializationContext());
            schedule.iCalendarContent = serializer.SerializeToString(calendar);
            return schedule;
        }

        private Schedule CreateMonthlySchedule( DateTime startDate, LocalTime startTime, LocalTime endTime )
        {
            var schedule = new Schedule();
            schedule.EffectiveStartDate = startDate;

            var endOfSession = new DateTime( startDate.Year, startDate.Month, startDate.Day, endTime.Hour, endTime.Minute, endTime.Second );
            var recurrenceRule = new RecurrencePattern( FrequencyType.Monthly, 1 );

            var e = new Event()
            {
                DtStart = new CalDateTime( startDate.AddHours( startTime.Hour ).AddMinutes( startDate.Minute ) ),
                DtEnd = new CalDateTime( endOfSession ),
                RecurrenceRules = new List<IRecurrencePattern> { recurrenceRule }
            };

            var calendar = new Ical.Net.Calendar();
            calendar.Events.Add( e );

            var serializer = new CalendarSerializer( new SerializationContext() );
            schedule.iCalendarContent = serializer.SerializeToString( calendar );
            return schedule;
        }

        private static void AddGroupAttribute( AttributeCache attribute, Group group, string attributeValue )
        {
            if ( !string.IsNullOrWhiteSpace( attributeValue ) )
            {
                group.Attributes.Add( attribute.Key, attribute );
                group.AttributeValues.Add( attribute.Key, new AttributeValueCache()
                {
                    AttributeId = attribute.Id,
                    Value = attributeValue
                } );
            }
        }

        /// <summary>
        /// Saves all family changes.
        /// </summary>
        private void SaveGroups( List<Group> newSmallGroups, Dictionary<int, GroupLocation> newGroupLocations )
        {
            var rockContext = new RockContext();

            // First save any unsaved families
            if ( newSmallGroups.Any() )
            {
                rockContext.WrapTransaction( ( ) =>
                {
                    rockContext.Groups.AddRange( newSmallGroups );
                    rockContext.SaveChanges( DisableAuditing );
                } );

                // Add groups to global list
                foreach (var newSmallGroup in newSmallGroups)
                {
                    ImportedSmallGroups.Add(newSmallGroup.ForeignId.Value, newSmallGroup);
                }
            }

            foreach (var group in newSmallGroups)
            {
                foreach ( var attributeCache in group.Attributes.Select( a => a.Value ) )
                {
                    var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == group.Id );
                    var newAttributeValue = group.AttributeValues[attributeCache.Key];

                    // set the new value and add it to the database
                    if ( existingValue == null )
                    {
                        existingValue = new AttributeValue();
                        existingValue.AttributeId = newAttributeValue.AttributeId;
                        existingValue.EntityId = group.Id;
                        existingValue.Value = newAttributeValue.Value;

                        rockContext.AttributeValues.Add( existingValue );
                    }
                    else
                    {
                        existingValue.Value = newAttributeValue.Value;
                        rockContext.Entry( existingValue ).State = EntityState.Modified;
                    }
                }
            }



            // Now save locations
            if ( newGroupLocations.Any() )
            {
                // Set updated family id on locations
                foreach ( var locationPair in newGroupLocations )
                {
                    int? groupId = ImportedSmallGroups.Where( k => k.Value.ForeignId == locationPair.Key ).Select( k => k.Value.Id ).FirstOrDefault();
                    locationPair.Value.GroupId = ( int )groupId;
                }

                // Save locations
                rockContext.WrapTransaction( ( ) =>
                {
                    rockContext.Configuration.AutoDetectChangesEnabled = false;
                    rockContext.GroupLocations.AddRange( newGroupLocations.Values );
                    rockContext.ChangeTracker.DetectChanges();
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }
    }
}