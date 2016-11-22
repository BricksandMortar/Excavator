using System;
using System.Collections.Generic;
using System.Data.Entity;
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
        /// <summary>
        /// Maps the company.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <returns></returns>
        private int MapCompany( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var businessList = new List<Group>();

            // Record status: Active, Inactive, Pending
            int? statusActiveId = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_ACTIVE ), lookupContext ).Id;

            // Record type: Business
            int? businessRecordTypeId = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS ), lookupContext ).Id;

            // Group role: Adult
            int groupRoleId = new GroupTypeRoleService( lookupContext ).Get( new Guid( Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT ) ).Id;

            // Group type: Family
            int familyGroupTypeId = GroupTypeCache.GetFamilyGroupType().Id;

            int completed = 0;
            ReportProgress( 0, "Starting company import");

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string householdIdKey = row[Company_Household_Id];
                int? householdId = householdIdKey.AsIntegerOrNull();
                if ( GetPersonKeys(null, householdId) == null )
                {
                    var businessGroup = new Group();
                    var businessPerson = new Person();

                    businessPerson.CreatedByPersonAliasId = ImportPersonAliasId;
                    businessPerson.CreatedDateTime = row[Company_Created_Date].AsDateTime() ?? RockDateTime.Now;
                    businessPerson.RecordTypeValueId = businessRecordTypeId;
                    businessPerson.RecordStatusValueId = statusActiveId;

                    var businessName = row[Company_Household_Name] as string;
                    if ( businessName != null )
                    {
                        businessName = businessName.Replace( "&#39;", "'" );
                        businessName = businessName.Replace( "&amp;", "&" );
                        businessPerson.LastName = businessName.Left( 50 );
                        businessGroup.Name = businessName.Left( 50 );
                    }

                    businessPerson.Attributes = new Dictionary<string, AttributeCache>();
                    businessPerson.AttributeValues = new Dictionary<string, AttributeValueCache>();
                    AddPersonAttribute( HouseholdIdAttribute, businessPerson, householdId.ToString() );

                    var groupMember = new GroupMember();
                    groupMember.Person = businessPerson;
                    groupMember.GroupRoleId = groupRoleId;
                    groupMember.GroupMemberStatus = GroupMemberStatus.Active;
                    businessGroup.Members.Add( groupMember );
                    businessGroup.GroupTypeId = familyGroupTypeId;
                    businessGroup.ForeignKey = householdId.ToString();
                    businessGroup.ForeignId = householdId;
                    businessList.Add( businessGroup );

                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1)
                    {
                        ReportProgress(0, string.Format( "{0:N0} companies imported", completed ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveCompanies( businessList );
                        ReportPartialProgress();
                        businessList.Clear();
                    }
                }
            }

            if ( businessList.Any() )
            {
                SaveCompanies( businessList );
            }

            ReportProgress( 100, string.Format( "Finished company import: {0:N0} companies imported.", completed ) );
            return completed;
        }

        /// <summary>
        /// Saves the companies.
        /// </summary>
        /// <param name="businessList">The business list.</param>
        private void SaveCompanies( List<Group> businessList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.Groups.AddRange( businessList );
                rockContext.SaveChanges( DisableAuditing );

                foreach ( var newBusiness in businessList )
                {
                    foreach ( var groupMember in newBusiness.Members )
                    {
                        // don't call LoadAttributes, it only rewrites existing cache objects
                        // groupMember.Person.LoadAttributes( rockContext );

                        foreach ( var attributeCache in groupMember.Person.Attributes.Select( a => a.Value ) )
                        {
                            var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == groupMember.Person.Id );
                            var newAttributeValue = groupMember.Person.AttributeValues[attributeCache.Key];

                            // set the new value and add it to the database
                            if ( existingValue == null )
                            {
                                existingValue = new AttributeValue();
                                existingValue.AttributeId = newAttributeValue.AttributeId;
                                existingValue.EntityId = groupMember.Person.Id;
                                existingValue.Value = newAttributeValue.Value;

                                rockContext.AttributeValues.Add( existingValue );
                            }
                            else
                            {
                                existingValue.Value = newAttributeValue.Value;
                                rockContext.Entry( existingValue ).State = EntityState.Modified;
                            }
                        }

                        if ( !groupMember.Person.Aliases.Any( a => a.AliasPersonId == groupMember.Person.Id ) )
                        {
                            groupMember.Person.Aliases.Add( new PersonAlias { AliasPersonId = groupMember.Person.Id, AliasPersonGuid = groupMember.Person.Guid } );
                        }

                        groupMember.Person.GivingGroupId = newBusiness.Id;
                    }
                }

                rockContext.ChangeTracker.DetectChanges();
                rockContext.SaveChanges( DisableAuditing );

                if ( businessList.Any() )
                {
                    var groupMembers = businessList.SelectMany( gm => gm.Members );
                    ImportedPeopleKeys.AddRange( groupMembers.Select( m => new PersonKeys
                    {
                        PersonAliasId = ( int ) m.Person.PrimaryAliasId,
                        PersonId = m.Person.Id,
                        IndividualId = null,
                        HouseholdId = m.Group.ForeignId,
                        FamilyRoleId = Utility.FamilyRole.Adult
                    } ).ToList()
                    );
                }
            } );
        }
        private const int Company_Household_Name = 1;
        private const int Company_Household_Id = 0;
        private const int Company_Contact_Name = 3;
        private const int Company_Created_Date = 4;
    }
}