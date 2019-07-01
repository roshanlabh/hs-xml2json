// dependencies
const async = require('async');
const AWS = require('aws-sdk');
const xmlParser = require('xml2json');
const util = require('util');

// get reference to S3 client 
const s3 = new AWS.S3();

exports.handler = function (event, context, callback) {
    // Read options from the event.
    console.log("Reading options from event:\n", util.inspect(event, { depth: 5 }));
    const srcBucket = event.Records[0].s3.bucket.name;      //get source bucket name

    // Object key may have spaces or unicode non-ASCII characters.
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log(`srcBucket: ${srcBucket}, srcKey: ${srcKey}`);

    // Infer the xml type.
    const typeMatch = srcKey.match(/\.([^.]*)$/);
    if (!typeMatch) {
        callback("Could not determine the xml type.");
        return;
    }

    const fileType = typeMatch[1].toLowerCase();    // file extension
    if (fileType != "xml") {
        callback(`Unsupported xml type: ${fileType}`);
        return;
    }

    // Download the xml from S3, transform and upload to a different S3 bucket.
    async.waterfall([
        function download(next) {
            // Download the xml from S3 into a buffer.
            s3.getObject({
                Bucket: srcBucket,
                Key: srcKey
            }, next);
        },
        function transform(response, next) {
            // console.log("transform > response: \n", response);
            const xmlJson = xmlParser.toJson(response.Body, { object: true });

            // loop through the reservations and create separate json of each reservation
            for (let resv of xmlJson.Reservations.Reservation) {
                // Destructure all the values from resv object
                let {
                    ID: bookingId,
                    Hotel: hotelCode,
                    From: fromDate,
                    To: toDate,
                    Guest: {
                        FirstName: guestFirstName,
                        LastName: guestLastName,
                        Address: {
                            Street: guestAddressStreet,
                            City: guestAddressCity,
                            CountryCode: guestAddressCountry
                        }
                    },
                    RoomStays: {
                        RoomStay: {
                            Room: {
                                ID: roomCode,
                                Occupancy: {
                                    Adults: adultCount,
                                    Children: childrenCount
                                }
                            },
                            Rates: {
                                Plan: rateCode,
                                Rate: rates
                            }

                        }
                    },
                    Comments: comments
                } = resv;

                const dstBucket = srcBucket.replace('input', 'output') + '/' + hotelCode;       // naming destination bucket
                const dstKey = `reservation_${bookingId}.json`;

                // get sum of rate.Amount
                const totalPrice = rates.reduce((sum, obj) => {
                    return sum + parseInt(obj.Amount);
                }, 0);
                const currency = rates[0].Currency;     //get currency

                const reservationData = {
                    "reservationId": bookingId,
                    "startDate": fromDate,
                    "endDate": toDate,
                    "guestRoomId": roomCode,
                    "ratePlanId": rateCode,
                    "totalPrice": totalPrice,
                    "currency": currency,
                    "guestCount": {
                        "adults": adultCount,
                        "children": childrenCount
                    },
                    "guestDetails": {
                        "firstName": guestFirstName,
                        "lastName": guestLastName,
                        "address": [guestAddressStreet, guestAddressCity, guestAddressCountry].join(', '),
                        "comments": [
                            comments
                        ]
                    }
                };
                console.log(`reservationData: `, reservationData);

                const reservationJson = JSON.stringify(reservationData, null, 2);

                // Upload the processed reservation json to a different S3 bucket.
                s3.putObject({
                    // ACL: 'public-read',
                    Bucket: dstBucket,
                    Key: dstKey,
                    Body: reservationJson,
                    ContentType: 'application/json'
                }, (err, data) => {
                    if (err) console.log(`putObject error: `, err.message);
                    else
                        console.log(`Successfully uploaded to ${dstBucket}/${dstKey}`);
                });
            }
            next();
        }
    ], function (err) {
        if (err) console.error('Unable to process: ', err);

        callback(null, "Done");
    }
    );
};
