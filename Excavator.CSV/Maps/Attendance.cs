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
using NodaTime.Text;
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
        private int MapAttendance( CSVInstance csvData )
        {
            var rockContext = new RockContext();

            // Set the supported date formats
            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy", "M/dd/yyyy", "M/d/yyyy" };
            var attendances = new List<Attendance>();
            var groupService = new GroupService(rockContext);
            var personService = new PersonService(rockContext);

            int completed = 0;
            ReportProgress( 0, "Starting Attendance import " );

            string[] row;
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                // TODO ROWs need to change
                var groupId = row[1].AsIntegerOrNull();
                var individualId = row[0].AsIntegerOrNull();
                string startDateUnparsed = row[2];
                var startDate = DateTime.ParseExact(startDateUnparsed, dateFormats, new CultureInfo( "en-US" ), DateTimeStyles.None);
                var startTimeUnparsed = row[3];
                var localTime = LocalTimePattern.CreateWithInvariantCulture( "H:mm" ).Parse( startTimeUnparsed );

                if (localTime == null)
                {
                    throw new Exception("Time could not be parsed");
                }

                var startDateTime = startDate.AddHours(localTime.Value.Hour).AddMinutes(localTime.Value.Minute);
                if (!groupId.HasValue || !individualId.HasValue)
                {
                    continue;
                }

                var group = groupService.Get(groupId.Value);
                var person = personService.Queryable().FirstOrDefault(p => p.ForeignId == individualId);

                if (group == null || person == null)
                {
                    continue;
                }

                var groupLocation = group.GroupLocations.FirstOrDefault(gl => gl.Schedules.Count > 0);
                if (groupLocation == null)
                {
                    continue;
                }

                var schedule = groupLocation.Schedules.FirstOrDefault();

                var attendance = new Attendance();
                attendance.GroupId = groupId;
                attendance.LocationId = groupLocation.LocationId;
                // ReSharper disable once PossibleNullReferenceException
                attendance.ScheduleId = schedule.Id;
                attendance.DidAttend = true;
                attendance.StartDateTime = startDateTime;

                completed++;
                if (completed%(ReportingNumber*10) < 1)
                {
                    ReportProgress(0, string.Format("{0:N0} attendance imported.", completed));
                }
                else if (completed%ReportingNumber < 1)
                {
                    SaveChanges( attendances, rockContext);

                    ReportPartialProgress();
                    attendances.Clear();

                    rockContext.SaveChanges(DisableAuditing);
                    // Reset lookup context
                    rockContext = new RockContext();

                    groupService = new GroupService(rockContext);
                    personService = new PersonService(rockContext);
                }
            }
            
            SaveChanges( attendances, rockContext );


            ReportProgress( 0, string.Format( "Finished attendance import: {0:N0} rows processed", completed ) );
            return completed;
        }
        
        private static void SaveChanges( List<Attendance> attendances, RockContext rockContext )
        {
            rockContext.WrapTransaction( () =>
            {
                rockContext.Attendances.AddRange( attendances );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
        #endregion Main Methods
    }
}