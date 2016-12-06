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
            int personTimelineNote = new NoteTypeService( lookupContext ).Get( new Guid( Rock.SystemGuid.NoteType.PERSON_TIMELINE_NOTE ) ).Id;
            var previouslyImportedNotes = new NoteService(lookupContext).Queryable().Where(n => n.ForeignId.HasValue && n.NoteTypeId == personTimelineNote).ToDictionary( t => ( int ) t.ForeignId, t => ( int? ) t.Id );
            var notes = new List<Note>();

            int completed = 0;

            ReportProgress( 0, string.Format( "Starting Status Advance Decline import ({0:N0} already exist).", 0 ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {

                string rowIndividualIdKey = row[NOTE_INDIVIDUAL_ID];
                var rowIndividualId = rowIndividualIdKey.AsType<int?>();
                string rowAttributeName = row[NOTE_ATTRIBUTE_NAME];
                string rowComment = row[NOTE_COMMENT];
                string rowNoteIdKey = row[NOTE_ID];
                var rowNoteId = row[NOTE_ID].AsIntegerOrNull();

                int? personId = null;
                var personKeys = GetPersonKeys( rowIndividualId );
                if ( personKeys == null )
                {
                    personId = personService.Queryable().FirstOrDefault( p => p.ForeignKey == rowIndividualIdKey )?.Id;
                }
                else if ( personKeys.PersonId > 0 )
                {
                    personId = personKeys.PersonId;
                }

                if ( ( rowNoteId.HasValue && previouslyImportedNotes.ContainsKey(rowNoteId.Value)) || personId == null || string.IsNullOrWhiteSpace(rowComment) || rowAttributeName == null)
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
                DateTime noteDateTime;
                bool successfulDateParse = DateTime.TryParse(row[NOTE_START_DATE], out noteDateTime);
                note.Text = splitComment[1];
                note.EntityId = personId;
                note.NoteTypeId = personTimelineNote;
                note.Caption = rowAttributeName;
                note.CreatedByPersonAliasId = ImportPersonAliasId;
                note.CreatedDateTime = successfulDateParse ? noteDateTime : ImportDateTime;
                note.ForeignKey = rowNoteIdKey;
                note.ForeignId = rowNoteId;
                notes.Add( note );

                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} status advance decline notes imported.", completed ) );
                }
                else if ( completed % ReportingNumber < 1 )
                {
                    SaveNotes( notes);
                    ReportPartialProgress();

                    // Reset lookup context
                    lookupContext = new RockContext();
                    personService = new PersonService(lookupContext);
                    notes.Clear();
                }
            }

            // Check to see if any rows didn't get saved to the database
            if ( notes.Any() )
            {
                SaveNotes( notes );
            }

            ReportProgress( 0, string.Format( "Finished status advanced decline import: {0:N0} person notes added.", completed ) );
            return completed;
        }

        private static void SaveNotes( List<Note> notes )
        {

            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Notes.AddRange( notes );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        private const int NOTE_INDIVIDUAL_ID = 0;
        private const int NOTE_ATTRIBUTE_NAME = 2;
        private const int NOTE_START_DATE = 3;
        private const int NOTE_COMMENT = 5;
        private const int NOTE_ID = 6;
    }
}