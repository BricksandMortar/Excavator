using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
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
        private int? _recordStatusPendingId;

        private readonly DefinedValueCache _homeNumberValue =
            DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_HOME ) );

        private int _groupRoleId;

        /// <summary>
        /// Maps the company.
        /// </summary>
        /// <param name="csvData"></param>
        /// <returns></returns>
        private int MapCompany( CSVInstance csvData )
        {

            var rockContext = new RockContext();
            var newGroupLocations = new Dictionary<string, GroupLocation>();
            var businessList = new List<BusinessCarrier>();
            var locationService = new LocationService( rockContext );

            int homeLocationTypeId = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_HOME ) ).Id;

            // Record status: Active, Inactive, Pending
            int? statusActiveId =
                DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_ACTIVE ), rockContext )
                                 .Id;

            // Record type: Business
            int? businessRecordTypeId =
                DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS ), rockContext )
                                 .Id;

            // Group role: Adult
            _groupRoleId =
                new GroupTypeRoleService( rockContext ).Get(
                                                           new Guid(
                                                               Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT ) )
                                                       .Id;

            // Group type: Family
            int familyGroupTypeId = GroupTypeCache.GetFamilyGroupType().Id;
            _recordStatusPendingId =
                DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_PENDING ),
                                     rockContext ).Id;

            int completed = 0;
            ReportProgress( 0, "Starting company import" );

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string householdIdKey = row[COMPANY_HOUSEHOLD_ID];
                int? householdId = householdIdKey.AsIntegerOrNull();
                if ( GetPersonKeys( null, householdId ) == null )
                {
                    var businessGroup = new Group();
                    var businessPerson = new Person
                    {
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        CreatedDateTime = DateTime.Now,
                        RecordTypeValueId = businessRecordTypeId,
                        RecordStatusValueId = statusActiveId
                    };


                    string businessName = row[COMPANY_HOUSEHOLD_NAME];
                    if ( businessName != null )
                    {
                        businessName = businessName.Replace( "&#39;", "'" );
                        businessName = businessName.Replace( "&amp;", "&" );
                        businessPerson.LastName = businessName.Left( 50 );
                        businessGroup.Name = businessName.Left( 50 );
                    }

                    businessPerson.Attributes = new Dictionary<string, AttributeCache>();
                    businessPerson.AttributeValues = new Dictionary<string, AttributeValueCache>();

                    var groupMember = new GroupMember
                    {
                        Person = businessPerson,
                        GroupRoleId = _groupRoleId,
                        GroupMemberStatus = GroupMemberStatus.Active
                    };
                    businessGroup.Members.Add( groupMember );
                    businessGroup.GroupTypeId = familyGroupTypeId;
                    businessGroup.ForeignKey = householdId.ToString();
                    businessGroup.ForeignId = householdId;

                    var contactGroup = GetBusinessContactFamilyGroup( row );
                    if (contactGroup != null)
                    {
                        contactGroup.ForeignKey = householdIdKey;
                    }
                    businessList.Add( new BusinessCarrier( contactGroup, businessGroup ) );
                    string address1 = row[CONTACT_ADDRESS_1];
                    string address2 = row[CONTACT_ADDRESS_2];
                    string city = row[CONCTACT_CITY];
                    string zip = row[CONTACT_POSTAL];

                    var primaryAddress = locationService.Get( address1, address2, city, null, zip, null, verifyLocation: false );

                    if ( primaryAddress != null )
                    {
                        var primaryLocation = new GroupLocation();
                        primaryLocation.LocationId = primaryAddress.Id;
                        primaryLocation.IsMailingLocation = true;
                        primaryLocation.IsMappedLocation = true;
                        primaryLocation.GroupLocationTypeValueId = homeLocationTypeId;
                        newGroupLocations.Add( businessGroup.ForeignKey, primaryLocation );
                    }

                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} companies imported", completed ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveCompanies( businessList, newGroupLocations );
                        ReportPartialProgress();
                        businessList.Clear();
                    }
                }
            }

            if ( businessList.Any() )
            {
                SaveCompanies( businessList, newGroupLocations );
            }

            ReportProgress( 100, string.Format( "Finished company import: {0:N0} companies imported.", completed ) );
            return completed;
        }

        /// <summary>
        /// Saves the companies.
        /// </summary>
        /// <param name="businessList">The business list.</param>
        private void SaveCompanies(List<BusinessCarrier> businessList, Dictionary<string, GroupLocation> newGroupLocations )
        {
            var rockContext = new RockContext();
            var importedGroupContacts = new List<Group>();
            rockContext.WrapTransaction(() =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;

                rockContext.Groups.AddRange( businessList.Where(bc => bc.ContactFamily != null).Select( bc => bc.ContactFamily ) );
                rockContext.Groups.AddRange(businessList.Select(bc => bc.BusinessGroup));
                rockContext.SaveChanges(DisableAuditing);

                var groupMemberService = new GroupMemberService( rockContext );
                var groupTypeRoleService = new GroupTypeRoleService( rockContext );
                int businessContactRole =
                    groupTypeRoleService.Get(
                                            new Guid(
                                                Rock.SystemGuid.GroupRole
                                                    .GROUPROLE_KNOWN_RELATIONSHIPS_BUSINESS_CONTACT ) ).Id;

                foreach ( var businessCarrier in businessList)
                {
                    importedGroupContacts.Add( businessCarrier.BusinessGroup );
                    foreach (var groupMember in businessCarrier.BusinessGroup.Members)
                    {
                        if (groupMember.Person.Aliases.All(a => a.AliasPersonId != groupMember.Person.Id))
                        {
                            groupMember.Person.Aliases.Add(new PersonAlias
                                       {
                                           AliasPersonId = groupMember.Person.Id,
                                           AliasPersonGuid = groupMember.Person.Guid
                                       });
                        }

                        groupMember.Person.GivingGroupId = businessCarrier.BusinessGroup.Id;
                    }

                    if (businessCarrier.ContactFamily != null)
                    {
                        foreach (var groupMember in businessCarrier.ContactFamily.Members)
                        {
                            if (groupMember.Person.Aliases.All(a => a.AliasPersonId != groupMember.Person.Id))
                            {
                                groupMember.Person.Aliases.Add(new PersonAlias
                                           {
                                               AliasPersonId = groupMember.Person.Id,
                                               AliasPersonGuid = groupMember.Person.Guid
                                           });
                            }

                            groupMember.Person.GivingGroupId = businessCarrier.ContactFamily.Id;
                        }

                        if (businessCarrier.ContactFamily != null)
                        {
                            int businessPersonId = businessCarrier.BusinessGroup.Members.FirstOrDefault().Person.Id;
                            foreach (var member in businessCarrier.ContactFamily.Members)
                            {
                                groupMemberService.CreateKnownRelationship(businessPersonId, member.Person.Id, businessContactRole );
                            }
                        }
                    }

                    rockContext.ChangeTracker.DetectChanges();
                    rockContext.SaveChanges(DisableAuditing);
                }
            });


            // Now save locations
//            if ( newGroupLocations.Any() )
//            {
//                // Set updated family id on locations
//                foreach ( var locationPair in newGroupLocations )
//                {
//                    var familyGroupId = importedGroupContacts.Where( g => g.ForeignKey == locationPair.Key ).Select( g => ( int? ) g.Id ).FirstOrDefault();
//                    if (familyGroupId.HasValue)
//                    {
//
//                        locationPair.Value.GroupId = ( int ) familyGroupId;
//                    }
//                }
//
//                // Save locations
//                rockContext.WrapTransaction( () =>
//                {
//                    rockContext.Configuration.AutoDetectChangesEnabled = false;
//                    rockContext.GroupLocations.AddRange( newGroupLocations.Values );
//                    rockContext.ChangeTracker.DetectChanges();
//                    rockContext.SaveChanges( DisableAuditing );
//                } );
//            }
        }

        private Group CreateFamilyGroup( string rowFamilyName )
        {
            var familyGroup = new Group
            {
                Name = rowFamilyName,
                CreatedByPersonAliasId = ImportPersonAliasId,
                GroupTypeId = FamilyGroupTypeId
            };
            return familyGroup;
        }

        private Group GetBusinessContactFamilyGroup( string[] row )
        {
            string contactName = row[COMPANY_CONTACT_NAME];
            if ( string.IsNullOrWhiteSpace( contactName ) )
            {
                return null;
            }
            
            var splitName = contactName.Split( ' ' );
            var contactPerson = new Person();
            contactPerson.FirstName = splitName[0].Left( 50 );
            contactPerson.NickName = splitName[0].Left( 50 );
            contactPerson.LastName = splitName.Length > 1 ? splitName[1].Left( 50 ) : String.Empty;
            contactPerson.SystemNote = string.Format(
                "Imported via Excavator Company Import as the contact for {0} on {1}", row[0],
                ImportDateTime );
            contactPerson.CreatedByPersonAliasId = ImportPersonAliasId;
            contactPerson.CreatedDateTime = ImportDateTime;
            contactPerson.ModifiedDateTime = ImportDateTime;
            contactPerson.Gender = Rock.Model.Gender.Unknown;
            contactPerson.RecordStatusValueId = _recordStatusPendingId;

            string number = !string.IsNullOrWhiteSpace( row[CONTACT_NUMBER_1] )
                ? row[CONTACT_NUMBER_1]
                : row[CONTACT_NUMBER_2];

            //Copied and pasted for Individual CSV
            if ( !string.IsNullOrWhiteSpace( number ) )
            {
                var extension = string.Empty;
                var countryCode = PhoneNumber.DefaultCountryCode();
                string normalizedNumber;
                var countryIndex = number.IndexOf( '+' );
                int extensionIndex = number.LastIndexOf( 'x' ) > 0 ? number.LastIndexOf( 'x' ) : number.Length;
                if ( countryIndex >= 0 )
                {
                    countryCode = number.Substring( countryIndex, countryIndex + 3 ).AsNumeric();
                    normalizedNumber =
                        number.Substring( countryIndex + 3, extensionIndex - 3 )
                              .AsNumeric()
                              .TrimStart( new Char[] { '0' } );
                    extension = number.Substring( extensionIndex );
                }
                else if ( extensionIndex > 0 )
                {
                    normalizedNumber = number.Substring( 0, extensionIndex ).AsNumeric();
                    extension = number.Substring( extensionIndex ).AsNumeric();
                }
                else
                {
                    normalizedNumber = number.AsNumeric();
                }

                if ( !string.IsNullOrWhiteSpace( normalizedNumber ) )
                {
                    var phoneNumber = new PhoneNumber();
                    phoneNumber.CountryCode = countryCode;
                    phoneNumber.CreatedByPersonAliasId = ImportPersonAliasId;
                    phoneNumber.Extension = extension.Left( 20 );
                    phoneNumber.Number = normalizedNumber.TrimStart('0').Left( 20 );
                    phoneNumber.NumberFormatted = PhoneNumber.FormattedNumber( phoneNumber.CountryCode,
                        phoneNumber.Number );
                    phoneNumber.NumberTypeValueId = _homeNumberValue.Id;
                    contactPerson.PhoneNumbers.Add( phoneNumber );
                }
            }
            contactPerson.EmailPreference = EmailPreference.EmailAllowed;
            contactPerson.Attributes = new Dictionary<string, AttributeCache>();
            contactPerson.AttributeValues = new Dictionary<string, AttributeValueCache>();

            var groupMember = new GroupMember
            {
                Person = contactPerson,
                GroupRoleId = _groupRoleId,
                CreatedDateTime = ImportDateTime,
                ModifiedDateTime = ImportDateTime,
                CreatedByPersonAliasId = ImportPersonAliasId,
                GroupMemberStatus = GroupMemberStatus.Active
            };

            var family = CreateFamilyGroup( contactName );
            family.Members.Add( groupMember );
            return family;
        }

        internal class BusinessCarrier
        {
            public Group ContactFamily { get; set; }
            public Group BusinessGroup { get; set; }

            public BusinessCarrier( Group contactFamily, Group businessGroup )
            {
                ContactFamily = contactFamily;
                BusinessGroup = businessGroup;
            }
        }

        private const int COMPANY_HOUSEHOLD_NAME = 0;
        private const int COMPANY_HOUSEHOLD_ID = 1;
        private const int COMPANY_CONTACT_NAME = 3;
        private const int CONTACT_ADDRESS_1 = 4;
        private const int CONTACT_ADDRESS_2 = 5;
        private const int CONCTACT_CITY = 6;
        private const int CONTACT_POSTAL = 7;
        private const int CONTACT_NUMBER_1 = 8;
        private const int CONTACT_NUMBER_2 = 9;
    }
}