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
using Attribute = Rock.Model.Attribute;

namespace Excavator.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the family import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        /// <summary>
        /// Loads the individual data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int MapGroupMember( CSVInstance csvData )
        {
            var rockContext = new RockContext();

            // Set the supported date formats
            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy", "M/dd/yyyy", "M/d/yyyy" };

            int completed = 0;
            ReportProgress( 0, "Starting Group Member import " );

            var personService = new PersonService( rockContext );
            var groupService = new GroupService( rockContext );
            var groupTypeRoleService = new GroupTypeRoleService( rockContext );
//            var attributeService = new AttributeService( rockContext );
            var groupMemberService = new GroupMemberService( rockContext );


            var newGroupMembers = new List<GroupMember>();
//            int personEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.Person ) ).Id;
            int groupMemberEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.GroupMember ) ).Id;
//            int groupEntityTypeId = EntityTypeCache.Read( typeof( Rock.Model.Group ) ).Id;
//            int addToGroupCategoryId = new CategoryService( rockContext ).Queryable().FirstOrDefault( c => c.Name == "Group Membership" ).Id;

            int smallGroupGroupTypeId = GroupTypeCache.Read( Rock.SystemGuid.GroupType.GROUPTYPE_SMALL_GROUP ).Id;

            if (ImportedSmallGroups == null || !ImportedSmallGroups.Any())
            {
                ImportedSmallGroups = new GroupService( rockContext ).Queryable().AsNoTracking()
               .Where( c => c.ForeignId != null && c.GroupTypeId == smallGroupGroupTypeId && c.ForeignKey == EXCAVATOR_IMPORTED_GROUP )
               // ReSharper disable once PossibleInvalidOperationException
               .ToDictionary( t => ( int ) t.ForeignId, t => t );
            }

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var f1GroupId = row[0].AsIntegerOrNull();
                var individualId = row[7].AsIntegerOrNull();
                if (!f1GroupId.HasValue || !individualId.HasValue)
                {
                    continue;
                }
                var group = ImportedSmallGroups.FirstOrDefault(d => d.Key == f1GroupId).Value ??
                            groupService.Queryable()
                                        .FirstOrDefault(
                                            c =>
                                                c.ForeignId != null && c.GroupTypeId == smallGroupGroupTypeId &&
                                                c.ForeignKey == EXCAVATOR_IMPORTED_GROUP);

                var person = personService.Queryable().FirstOrDefault(p => p.ForeignId == individualId);

                if (person == null || group == null)
                {
                    continue;
                }
               
                string grouproleName = row[6];

                if (string.IsNullOrWhiteSpace(grouproleName))
                {
                    continue;
                }

//                var attributes =
//                    attributeService.GetByEntityTypeId(groupMemberEntityTypeId)
//                                    .Where(a => a.EntityTypeQualifierValue == smallGroupGroupTypeId.ToString())
//                                    .ToList();

                var groupRole = groupTypeRoleService.GetByGroupTypeId(smallGroupGroupTypeId)
                                                    .FirstOrDefault(r => r.Name == grouproleName);

                bool loadedFromMemory = true;
                var groupMember = newGroupMembers.FirstOrDefault(gm => gm.PersonId == person.Id);
                if (groupMember == null)
                {
                    loadedFromMemory = false;
                    groupMember =
                        groupMemberService.GetByGroupIdAndPersonId( group.Id, person.Id).FirstOrDefault();
                }
                if (groupMember == null)
                {
                    groupMember = new GroupMember
                    {
                        GroupId = group.Id,
                        PersonId = person.Id,
                        GroupRoleId = groupRole.Id,
                        DateTimeAdded =
                            DateTime.ParseExact(row[2], dateFormats, CultureInfo.InvariantCulture,
                                DateTimeStyles.None),
                        GroupMemberStatus = GroupMemberStatus.Active
                    };
                    groupMember.Attributes = new Dictionary<string, AttributeCache>();
                    groupMember.AttributeValues = new Dictionary<string, AttributeValueCache>();
                    newGroupMembers.Add(groupMember);
                }
                else
                {
                    if (!loadedFromMemory)
                    {
                        groupMember.LoadAttributes(rockContext);
                    }
                }

                completed++;
                if (completed%(ReportingNumber*10) < 1)
                {
                    ReportProgress(0, string.Format("{0:N0} group members imported.", completed));
                }
                else if (completed%ReportingNumber < 1)
                {
                    SaveChanges( newGroupMembers, rockContext);

                    ReportPartialProgress();

                    rockContext.SaveChanges(DisableAuditing);
                    // Reset lookup context
                    rockContext = new RockContext();
                    newGroupMembers.Clear();

                    groupMemberService = new GroupMemberService(rockContext);
                    personService = new PersonService(rockContext);
                    groupService = new GroupService(rockContext);
                    groupTypeRoleService = new GroupTypeRoleService(rockContext);
                }
            }
            
            SaveChanges( newGroupMembers, rockContext );


            ReportProgress( 0, string.Format( "Finished group member import: {0:N0} rows processed", completed ) );
            return completed;
        }

        /// <summary>
        /// Saves the individuals.
        /// </summary>
        /// <param name="newFamilyList">The family list.</param>
        /// <param name="visitorList">The optional visitor list.</param>
        private void SaveChanges( List<GroupMember> newGroupMembers, RockContext rockContext )
        {
            rockContext.WrapTransaction( () =>
            {
                rockContext.GroupMembers.AddRange( newGroupMembers );
                rockContext.SaveChanges( DisableAuditing );
                // new group members
                foreach ( var groupMember in newGroupMembers )
                {
                    foreach ( var attributeCache in groupMember.Attributes.Select( a => a.Value ) )
                    {
                        var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == groupMember.Id );
                        var newAttributeValue = groupMember.AttributeValues[attributeCache.Key];

                        // set the new value and add it to the database
                        if ( existingValue == null )
                        {
                            existingValue = new AttributeValue();
                            existingValue.AttributeId = newAttributeValue.AttributeId;
                            existingValue.EntityId = groupMember.Id;
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
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
//
//        private static void AddGroupMemberAttribute( string attributeKey, GroupMember groupMember, string attributeValue, IEnumerable<Attribute> attributes )
//        {
//            // get attribute
//            var attributeModel = attributes.FirstOrDefault( a => a.Key == attributeKey );
//            if ( attributeModel == null )
//            {
//                string message = "Expected " + attributeKey +
//                " attribute to exist for " + groupMember.Group.GroupType.Name;
//                throw new Exception( message );
//            }
//            var attribute = AttributeCache.Read( attributeModel );
//
//            if ( attribute != null && !string.IsNullOrWhiteSpace( attributeValue ) )
//            {
//                Console.WriteLine( "Added attribute" );
//                if ( groupMember.Attributes.ContainsKey( attribute.Key ) )
//                {
//                    groupMember.AttributeValues[attribute.Key] = new AttributeValueCache()
//                    {
//                        AttributeId = attribute.Id,
//                        Value = attributeValue
//                    };
//                }
//                else
//                {
//                    groupMember.Attributes.Add( attribute.Key, attribute );
//                    groupMember.AttributeValues.Add( attribute.Key, new AttributeValueCache()
//                    {
//                        AttributeId = attribute.Id,
//                        Value = attributeValue
//                    } );
//                }
//
//            }
//        }
//
//        private static void UpdateGroupMemberAttribute( string attributeKey, GroupMember groupMember, string attributeValue, IEnumerable<Attribute> attributes )
//        {
//            var attributeModel = attributes.FirstOrDefault( a => a.Key == attributeKey );
//            if ( attributeModel == null )
//            {
//                string message = "Expected " + attributeKey +
//                " attribute to exist for " + groupMember.Group.GroupType.Name;
//                throw new Exception( message );
//            }
//            var attributeCache = AttributeCache.Read( attributeModel );
//            if ( attributeCache != null && !string.IsNullOrWhiteSpace( attributeValue ) )
//            {
//                if ( groupMember.Attributes.ContainsKey( attributeCache.Key ) )
//                {
//                    groupMember.AttributeValues[attributeCache.Key] = new AttributeValueCache()
//                    {
//                        AttributeId = attributeCache.Id,
//                        Value = groupMember.AttributeValues[attributeCache.Key].Value + "," + attributeValue
//                    };
//                }
//                else
//                {
//                    groupMember.Attributes.Add( attributeCache.Key, attributeCache );
//                    groupMember.AttributeValues.Add( attributeCache.Key, new AttributeValueCache()
//                    {
//                        AttributeId = attributeCache.Id,
//                        Value = attributeValue
//                    } );
//                }
//
//            }
//        }

        #endregion Main Methods
    }
}