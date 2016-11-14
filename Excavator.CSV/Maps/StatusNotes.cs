using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text.RegularExpressions;
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
        /// Loads the family data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int MapNotes( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var personService = new PersonService( lookupContext );
            var statusNoteTypeId = new NoteTypeService( lookupContext ).Get( new Guid( Rock.SystemGuid.NoteType.PERSON_TIMELINE_NOTE ) ).Id;
            var notes = new List<Note>();

            int completed = 0;

            ReportProgress( 0, string.Format( "Starting metrics import ({0:N0} already exist).", 0 ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {

                string rowIndividualIdKey = row[NoteIndividualId];
                int? rowIndividualId = rowIndividualIdKey.AsType<int?>();
                string rowAttributeName = row[NoteAttributeName];
                string rowComment = row[NoteComment];

                int? personId = null;
                var personKeys = GetPersonKeys( rowIndividualId );
                if ( personKeys == null )
                {
                    personId = personService.Queryable().FirstOrDefault( p => p.ForeignKey == rowIndividualIdKey ).Id;
                }
                else if ( personKeys != null && personKeys.PersonId > 0 )
                {
                    personId = personKeys.PersonId;
                }

                if (personId == null || string.IsNullOrWhiteSpace(rowComment) || rowAttributeName == null)
                {
                    continue;
                }

                // create note
                var note = new Note();
                var splitComment = Regex.Split(rowComment, "instance\\.\\s");
                if (splitComment.Length < 2)
                {
                    continue;
                }
                note.Text = rowAttributeName + ": " + splitComment[1];
                note.EntityId = personId;
                note.NoteTypeId = statusNoteTypeId;
                note.Caption = rowAttributeName;
                notes.Add( note );

                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} metrics imported.", completed ) );
                }
                else if ( completed % ReportingNumber < 1 )
                {
                    SaveNotes( notes);
                    ReportPartialProgress();

                    // Reset lookup context
                    lookupContext = new RockContext();
                    notes.Clear();
                }
            }

            // Check to see if any rows didn't get saved to the database
            if ( notes.Any() )
            {
                SaveNotes( notes );
            }

            ReportProgress( 0, string.Format( "Finished metrics import: {0:N0} metrics added or updated.", completed ) );
            return completed;
        }

        private void SaveNotes( List<Note> notes )
        {

            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Notes.AddRange( notes );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        private const int NoteIndividualId = 0;
        private const int NoteAttributeName = 2;
        private const int NoteStartDate = 3;
        private const int NoteComment = 5;
    }
}