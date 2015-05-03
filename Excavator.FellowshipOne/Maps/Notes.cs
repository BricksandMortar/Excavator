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
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Excavator.F1
{
    /// <summary>
    /// Partial of F1Component that holds the Notes import
    /// </summary>
    partial class F1Component
    {
        /// <summary>
        /// Maps the notes.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        public void MapNotes( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var categoryService = new CategoryService( lookupContext );
            var personService = new PersonService( lookupContext );

            var noteTypes = new NoteTypeService( lookupContext ).Queryable().ToList();
            int noteTimelineTypeId = noteTypes.Where( nt => nt.Guid == new Guid( Rock.SystemGuid.NoteType.PERSON_TIMELINE ) )
                .Select( nt => nt.Id ).FirstOrDefault();

            var importedUsers = new UserLoginService( lookupContext ).Queryable()
                .Where( u => u.ForeignId != null )
                .ToDictionary( t => t.ForeignId.AsType<int?>(), t => t.PersonId );

            var noteList = new List<Note>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying note import ({0:N0} found).", totalRows ) );
            foreach ( var row in tableData.Where( r => r != null ) )
            {
                string text = row["Note_Text"] as string;
                int? individualId = row["Individual_ID"] as int?;
                int? householdId = row["Household_ID"] as int?;
                var personKeys = GetPersonAliasId( individualId, householdId );
                if ( personKeys != null && !string.IsNullOrWhiteSpace( text ) )
                {
                    int? userId = row["NoteCreatedByUserID"] as int?;
                    if ( userId != null && importedUsers.ContainsKey( userId ) )
                    {
                        DateTime? dateCreated = row["NoteCreated"] as DateTime?;
                        string noteType = row["Note_Type_Name"] as string;

                        var note = new Note();
                        note.CreatedByPersonAliasId = (int)importedUsers[userId];
                        note.CreatedDateTime = dateCreated;
                        note.EntityId = personKeys.PersonId;
                        note.Text = text;

                        if ( !string.IsNullOrWhiteSpace( noteType ) )
                        {
                            int? noteTypeId = noteTypes.Where( nt => nt.Name == noteType ).Select( i => (int?)i.Id ).FirstOrDefault();
                            note.NoteTypeId = noteTypeId ?? noteTimelineTypeId;
                        }
                        else
                        {
                            note.NoteTypeId = noteTimelineTypeId;
                        }

                        noteList.Add( note );
                        completed++;

                        if ( completed % percentage < 1 )
                        {
                            int percentComplete = completed / percentage;
                            ReportProgress( percentComplete, string.Format( "{0:N0} notes imported ({1}% complete).", completed, percentComplete ) );
                        }
                        else if ( completed % ReportingNumber < 1 )
                        {
                            SaveNotes( noteList );
                            ReportPartialProgress();
                            noteList.Clear();
                        }
                    }
                }
            }

            if ( noteList.Any() )
            {
                SaveNotes( noteList );
            }

            ReportProgress( 100, string.Format( "Finished note import: {0:N0} notes imported.", completed ) );
        }

        /// <summary>
        /// Saves the notes.
        /// </summary>
        /// <param name="noteList">The note list.</param>
        private void SaveNotes( List<Note> noteList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.Notes.AddRange( noteList );
                rockContext.SaveChanges( DisableAudit );
            } );
        }
    }
}