function parsePredictions(predictions) {
    var cities = {};
    var stations = {};
    var dateTimes = {};
    predictions.forEach((row) => {
        var dateTime = `${row.date} ${row.time}`;
        var dateArr = row.date.split("-").concat(row.time.split(":"));
        dateTimes[dateTime] = new Date(Date.UTC(
            dateArr[0], dateArr[1] - 1, dateArr[2], dateArr[3], dateArr[4]));

        if (!stations[row.city]) {
            stations[row.city] = {};
        }
        stations[row.city][row.name] = 1;

        if (!cities[row.city]) {
            cities[row.city] = {};
        }
        if (!cities[row.city][dateTime]) {
            cities[row.city][dateTime] = [];
        }
        cities[row.city][dateTime][row.name] = row;
    });
    Object.keys(stations).forEach((key) => {
        stations[key] = Object.keys(stations[key]);
        stations[key].sort();
    });
    var allDateTimes = Object.keys(dateTimes);
    allDateTimes.sort();

    console.log(predictions[0]);
    console.log(cities);
    console.log(dateTimes);
    console.log(stations);

    ReactDOM.render(
        <App
            allDateTimes={allDateTimes}
            cities={cities}
            dateTimes={dateTimes}
            stations={stations}
        />,
        document.getElementById('content')
    );
}

var App = React.createClass({
    renderTable: function(cityData, stationNames, range) {
        const station = stationNames[0];
        let currentDate = "";
        const dateRow = range.map((dateTime) => {
            const dateStr = this.props.dateTimes[dateTime].toLocaleFormat("%m/%d");
            if (dateStr !== currentDate) {
                currentDate = dateStr;
                return <td width="6.9%">{dateStr}</td>;
            }
            return <td width="6.9%" />;
        });
        dateRow.splice(0, 0, <td />);

        const timeRow = range.map((dateTime) => {
            const timeStr = this.props.dateTimes[dateTime].toLocaleFormat("%I:%M %p");
            return <td>{timeStr}</td>;
        });
        timeRow.splice(0, 0, <td />);

        const tempRow = range.map((dateTime) => {
            const val = cityData[dateTime][station].temperature;
            return <td>{val.toFixed()}°F</td>;
        });
        tempRow.splice(0, 0, <td>Temperature</td>);

        const humidRow = range.map((dateTime) => {
            const val = cityData[dateTime][station].humidity;
            return <td>{(val*100).toFixed()}%</td>;
        });
        humidRow.splice(0, 0, <td>Humidity</td>);

        const precipRow = range.map((dateTime) => {
            const val = cityData[dateTime][station].precipIntensity;
            return <td>{val.toFixed()} in</td>;
        });
        precipRow.splice(0, 0, <td>Rain</td>);

        const visRow = range.map((dateTime) => {
            const val = cityData[dateTime][station].visibility;
            return <td>{val.toFixed()} mi</td>;
        });
        visRow.splice(0, 0, <td>Visibility</td>);

        const windRow = range.map((dateTime) => {
            const val = cityData[dateTime][station].windSpeed;
            return <td style={{verticalAlign: "top"}}>{val.toFixed()} mph</td>;
        });
        windRow.splice(0, 0, <td style={{paddingBottom: "20px"}}>Wind speed</td>);

        const pressureRow = range.map((dateTime) => {
            const val = cityData[dateTime][station].pressure;
            return <td>{val.toFixed()} mb</td>;
        });
        pressureRow.splice(0, 0, <td>Barometer</td>);

        const stationRows = stationNames.map((station) => <tr className="station">
            <td>{station}</td>
            {range.map((dateTime) => (
                <td className={cityData[dateTime][station].prediction === "0" ?
                    "availableCell" : "unavailableCell"} />))}
        </tr>);

        return <table width="100%" style={{padding: "16px"}}><tbody>
            <tr style={{fontWeight: "bold"}}>{dateRow}</tr>
            <tr style={{fontWeight: "bold"}}>{timeRow}</tr>
            <tr style={{color:"#a93f02"}}>{tempRow}</tr>
            <tr style={{color:"#66a61e"}}>{humidRow}</tr>
            <tr style={{color:"#7570b3"}}>{precipRow}</tr>
            <tr style={{color:"#666666"}}>{pressureRow}</tr>
            <tr style={{color:"#1b9e77"}}>{visRow}</tr>
            <tr style={{color:"#a6761d"}}>{windRow}</tr>
            {stationRows}
        </tbody></table>;
    },

    renderCity: function(cityName) {
        const cityData = this.props.cities[cityName];
        const dateRange = this.props.allDateTimes.slice(0, 12);
        return <div>
            <img
                src={"/images/" + cityName + ".jpg"}
                style={{ position: "absolute", zIndex: -10 }}
                width="100%"
            />
            <h2 style={{padding: "16px", margin: 0}}>
                {cityName.replace("_", " ")}
            </h2>
            {this.renderTable(cityData, this.props.stations[cityName], dateRange)}
        </div>;
    },

    render: function() {
        const cityNames = Object.keys(this.props.cities);
        return <div>
            <h1 style={{textAlign: "center"}}>Project TX</h1>
            {cityNames.map(this.renderCity)}
        </div>;
    }
});

ReactDOM.render(
    <h1 style={{textAlign: "center"}}>Loading...</h1>,
    document.getElementById('content')
);

window.parsePredictions = parsePredictions;

var script = document.createElement("script");
script.setAttribute("src", "/data/latest_predictions.jsonp");
document.head.appendChild(script);
