var assert = require( 'assert' );
var Promise = require( 'bluebird' );

var mocksqs = require( './index' );

afterEach( function () {
    mocksqs.reset();
})

describe( '.createQueue', function () {

    it( 'sets the queue name and region', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS({
            region: 'hello'
        }) );
        sqs.createQueueAsync({
            QueueName: 'q1'
        })
        .then( function ( res ) {
            assert( res.QueueUrl );
            assert.equal( res.QueueUrl.split( '.' )[ 1 ], 'hello' );
            assert.equal( res.QueueUrl.split( '/' ).pop(), 'q1' )
            done();
        })
        .catch( done );
    })

    it( 'fails when queue name is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({})
            .then( function () {
                done( 'Error expected for required QueueName' )
            })
            .catch( function ( err ) {
                assert( err.toString().match( /QueueName.*required/ig ) )
                done();
            })
    })

    it( 'fails when queue name already exists', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1'
        })
        .then( function () {
            return sqs.createQueueAsync({
                QueueName: 'q1'
            })
        })
        .then( function () {
            done( 'Error expected for Queue already exists' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /Queue.*q1.*already exists/ig ) );
            done();
        })
    })

    it( 'overwrites the default attributes', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1',
            Attributes: {
                DelaySeconds: 30,
                MaximumMessageSize: 1024,
                MessageRetentionPeriod: 120,
                ReceiveMessageWaitTimeSeconds: 3,
                VisibilityTimeout: 45
            }
        })
        .then( function ( res ) {
            return sqs.getQueueAttributesAsync({
                QueueUrl: res.QueueUrl,
                AttributeNames: [ 'All' ]
            })
        })
        .then( function ( res ) {
            var attribs = res.Attributes;
            assert.equal( attribs.DelaySeconds, 30 );
            assert.equal( attribs.MaximumMessageSize, 1024 );
            assert.equal( attribs.MessageRetentionPeriod, 120 );
            assert.equal( attribs.ReceiveMessageWaitTimeSeconds, 3 );
            assert.equal( attribs.VisibilityTimeout, 45 );

            done();
        })
        .catch( done )
    })

})

describe( '.getQueueAttributes', function () {

    var qurl;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1',
        })
        .then( function ( res ) {
            qurl = res.QueueUrl
            done();
        })
        .catch( done )
    })

    it( 'returns specific attributes', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        return sqs.getQueueAttributesAsync({
            QueueUrl: qurl,
            AttributeNames: [ 'CreatedTimestamp', 'DelaySeconds' ]
        })
        .then( function ( res ) {
            var attribs = res.Attributes;
            assert.equal( Object.keys( attribs ).length, 2 );
            assert.notEqual( attribs.DelaySeconds, undefined );
            assert.equal( typeof attribs.CreatedTimestamp, 'number' );
            done();
        })
        .catch( done )
    })

    it( 'returns count approximations', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        return sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'hello world'
        })
        .then( function () {
            return sqs.sendMessageAsync({
                QueueUrl: qurl,
                MessageBody: 'foo bar'
            })
        })
        .then( function () {
            return sqs.receiveMessage({
                QueueUrl: qurl
            })
        })
        .then( function () {
            return sqs.getQueueAttributesAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            var attribs = res.Attributes;
            assert.equal( attribs.ApproximateNumberOfMessages, 1 );
            assert.equal( attribs.ApproximateNumberOfMessagesNotVisible, 1 );
            done();
        })
        .catch( done )
    })

    it( 'fails when queue url is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.getQueueAttributesAsync({})
            .then( function () {
                done( 'Error expected for required QueueUrl' )
            })
            .catch( function ( err ) {
                assert( err.toString().match( /QueueUrl.*required/ig ) )
                done();
            })
    })

    it( 'fails when queue doesn\'t exists', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.getQueueAttributesAsync({
            QueueUrl: 'hello'
        })
        .then( function () {
            done( 'Error expected for Queue doesn\'t exists' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /Queue.*doesn\'t exist.*hello/ig ) );
            done();
        })
    })

})

describe( '.listQueues', function () {

    var qurl1;
    var qurl2;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1-hello'
        })
        .then( function ( res ) {
            qurl1 = res.QueueUrl
            return sqs.createQueueAsync({
                QueueName: 'q2-world'
            })
        })
        .then( function ( res ) {
            qurl2 = res.QueueUrl;
            done();
        })
        .catch( done )
    });

    it( 'lists all of the queues', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.listQueuesAsync({})
            .then( function ( res ) {
                assert.equal( res.QueueUrls.length, 2 );
                assert.equal( res.QueueUrls[ 0 ], qurl1 );
                assert.equal( res.QueueUrls[ 1 ], qurl2 );
                done();
            })
            .catch( done );
    })

    it( 'search by prefix', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.listQueuesAsync({
            QueueNamePrefix: 'q2-'
        })
        .then( function ( res ) {
            assert.equal( res.QueueUrls.length, 1 );
            assert.equal( res.QueueUrls[ 0 ], qurl2 );
            done();
        })
        .catch( done );
    })

})

describe( '.deleteQueue', function () {

    var qurl;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1'
        })
        .then( function ( res ) {
            qurl = res.QueueUrl
            done();
        })
        .catch( done )
    })

    it( 'deletes the queue', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.deleteQueueAsync({
            QueueUrl: qurl
        })
        .then( function () {
            return sqs.listQueuesAsync({})
        })
        .then( function ( res ) {
            assert.equal( res.QueueUrls.length, 0 )
            done();
        })
        .catch( done )
    });

    it( 'quietly ignores missing queues', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.deleteQueueAsync({
            QueueUrl: 'hello'
        })
        .then( function () {
            return sqs.listQueuesAsync({})
        })
        .then( function ( res ) {
            assert.equal( res.QueueUrls.length, 1 )
            assert.equal( res.QueueUrls[ 0 ], qurl );
            done();
        })
        .catch( done )
    })
})

describe( '.sendMessage', function () {

    var qurl;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1'
        })
        .then( function ( res ) {
            qurl = res.QueueUrl
            done();
        })
        .catch( done )
    })

    it( 'generates unique message ids', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        var messageId;
        sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'hello world'
        })
        .then( function ( res ) {
            messageId = res.MessageId;
            assert( messageId );
            return sqs.sendMessageAsync({
                QueueUrl: qurl,
                MessageBody: 'foo bar'
            })
        })
        .then( function ( res ) {
            assert.notEqual( res.MessageId, messageId );
            done();
        })
        .catch( done );
    })

    it( 'fails when queue url is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.sendMessageAsync({
            MessageBody: 'hello world'
        })
        .then( function () {
            done( 'Error expected for required QueueUrl' )
        })
        .catch( function ( err ) {
            assert( err.toString().match( /QueueUrl.*required/ig ) )
            done();
        })
    })

    it( 'fails when message body is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.sendMessageAsync({
            QueueUrl: qurl
        })
        .then( function () {
            done( 'Error expected for required MessageBody' )
        })
        .catch( function ( err ) {
            assert( err.toString().match( /MessageBody.*required/ig ) )
            done();
        })
    })

    it( 'fails when queue doesn\'t exists', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.sendMessageAsync({
            QueueUrl: 'hello',
            MessageBody: 'hello world'
        })
        .then( function () {
            done( 'Error expected for Queue doesn\'t exists' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /Queue.*doesn\'t exist.*hello/ig ) );
            done();
        })
    })

    it( 'stores the message body', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'hello world'
        })
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            var body = res.Messages[ 0 ].Body;
            assert.equal( body, 'hello world' );
            done();
        })
        .catch( done )
    })

    it( 'stores the SentTime', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        var before = new Date().getTime();
        sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'hello world'
        })
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            var attribs = res.Messages[ 0 ].Attributes;

            var after = new Date().getTime();
            assert( attribs.SentTimestamp >= before );
            assert( attribs.SentTimestamp <= after );
            done();
        })
        .catch( done )
    })

    it( 'stores the message attributes', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'hello world',
            MessageAttributes: {
                foo: {
                    DataType: 'String',
                    StringValue: 'bar'
                }
            }
        })
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            var attribs = res.Messages[ 0 ].MessageAttributes;
            assert.deepEqual( attribs, {
                foo: {
                    DataType: 'String',
                    StringValue: 'bar'
                }
            })
            
            done();
        })
        .catch( done )
    })
})

describe( '.receiveMessage', function () {

    var qurl;
    var messageId;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1',
            Attributes: {
                VisibilityTimeout: 0
            }
        })
        .then( function ( res ) {
            qurl = res.QueueUrl
            return sqs.sendMessageAsync({
                QueueUrl: qurl,
                MessageBody: 'hello world',
            })
        })
        .then( function ( res ) {
            messageId = res.MessageId;
            done();
        })
        .catch( done )
    })

    it( 'receives the correct message id', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.receiveMessageAsync({
            QueueUrl: qurl
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 1 );
            assert.equal( res.Messages[ 0 ].MessageId, messageId );
            done();
        })
        .catch( done );
    })

    it( 'recieves a different receipt handle for each time', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        var receipt;
        sqs.receiveMessageAsync({
            QueueUrl: qurl
        })
        .then( function ( res ) {
            receipt = res.Messages[ 0 ].ReceiptHandle;
            assert( receipt );
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            assert.notEqual( res.Messages[ 0 ].ReceiptHandle, receipt );
            done();
        })
        .catch( done );
    })

    it( 'increments the receive count', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.receiveMessageAsync({
            QueueUrl: qurl
        })
        .then( function ( res ) {
            var attribs = res.Messages[ 0 ].Attributes;
            assert.equal( attribs.ApproximateReceiveCount, 1 )

            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            var attribs = res.Messages[ 0 ].Attributes;
            assert.equal( attribs.ApproximateReceiveCount, 2 )
            done();
        })
        .catch( done );
    })

    it( 'stores the first receive time', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        var before = new Date().getTime();
        var first;
        sqs.receiveMessageAsync({
            QueueUrl: qurl
        })
        .then( function ( res ) {
            var attribs = res.Messages[ 0 ].Attributes;
            first = attribs.ApproximateFirstReceiveTimestamp;
            assert( first >= before );
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            var attribs = res.Messages[ 0 ].Attributes;
            assert.equal( attribs.ApproximateFirstReceiveTimestamp, first )
            done();
        })
        .catch( done );
    })

    it( 'fails when queue url is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.receiveMessageAsync({})
            .then( function () {
                done( 'Error expected for required QueueUrl' )
            })
            .catch( function ( err ) {
                assert( err.toString().match( /QueueUrl.*required/ig ) )
                done();
            })
    })

    it( 'fails when queue doesn\'t exists', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.receiveMessageAsync({
            QueueUrl: 'hello',
        })
        .then( function () {
            done( 'Error expected for Queue doesn\'t exists' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /Queue.*doesn\'t exist.*hello/ig ) );
            done();
        })
    })

    it( 'fails when MaxMessages is out of range', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.receiveMessageAsync({
            QueueUrl: qurl,
            MaxNumberOfMessages: 11
        })
        .then( function () {
            done( 'Error expected for MaxMessages out of range' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /MaxNumberOfMessages.*out of range/ig ) );
            return sqs.receiveMessageAsync({
                QueueUrl: qurl,
                MaxNumberOfMessages: 0
            })
        })
        .then( function () {
            done( 'Error expected for MaxMessages out of range' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /MaxNumberOfMessages.*out of range/ig ) );
            done();
        });

    })

    it( 'receives a single message by default', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        var messageId2;
        return sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'foo bar',
        })
        .then( function ( res ) {
            messageId2 = res.MessageId;
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 1 );

            var msg = res.Messages[ 0 ];
            var ids = [ messageId, messageId2 ];
            assert.notEqual( ids.indexOf( msg.MessageId ), -1 )
            done();
        })
    })

    it( 'receives multiple messages', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        var messageId2;
        return sqs.sendMessageAsync({
            QueueUrl: qurl,
            MessageBody: 'foo bar',
        })
        .then( function ( res ) {
            messageId2 = res.MessageId;
            return sqs.receiveMessageAsync({
                QueueUrl: qurl,
                MaxNumberOfMessages: 5
            })
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 2 );
            done();
        })
    });

    it( 'hides received messages', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        return sqs.receiveMessageAsync({
            QueueUrl: qurl,
            VisibilityTimeout: .02
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 1 );
            return sqs.receiveMessageAsync({
                QueueUrl: qurl,
            })
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 0 );
        })
        .delay( 30 )
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl,
            })
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 1 );
            done();
        })
        .catch( done );
    })

})

describe( '.deleteMessage', function () {

    var qurl;
    var receipt;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1',
            Attributes: {
                VisibilityTimeout: 0
            }
        })
        .then( function ( res ) {
            qurl = res.QueueUrl
            return sqs.sendMessageAsync({
                QueueUrl: qurl,
                MessageBody: 'hello world',
            })
        })
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            receipt = res.Messages[ 0 ].ReceiptHandle;
            done();
        })
        .catch( done )
    })

    it( 'deletes a message', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        return sqs.deleteMessageAsync({
            QueueUrl: qurl,
            ReceiptHandle: receipt
        })
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 0 );
            done();
        })
        .catch( done );
    })

    it( 'quietly ignores missing receipts', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        return sqs.deleteMessageAsync({
            QueueUrl: qurl,
            ReceiptHandle: 'hello'
        })
        .then( function () {
            done();
        })
        .catch( done );
    })

    it( 'fails when queue url is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.deleteMessageAsync({
            ReceiptHandle: receipt
        })
        .then( function () {
            done( 'Error expected for required QueueUrl' )
        })
        .catch( function ( err ) {
            assert( err.toString().match( /QueueUrl.*required/ig ) )
            done();
        })
    })

    it( 'fails when receipt handle is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.deleteMessageAsync({
            QueueUrl: qurl,
        })
        .then( function () {
            done( 'Error expected for required ReceiptHandle' )
        })
        .catch( function ( err ) {
            assert( err.toString().match( /ReceiptHandle.*required/ig ) )
            done();
        })
    })

    it( 'fails when queue doesn\'t exists', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.deleteMessageAsync({
            QueueUrl: 'hello',
            ReceiptHandle: receipt
        })
        .then( function () {
            done( 'Error expected for Queue doesn\'t exists' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /Queue.*doesn\'t exist.*hello/ig ) );
            done();
        })
    })
})

describe( '.purgeQueue', function () {

    var qurl;
    var receipt;
    beforeEach( function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.createQueueAsync({
            QueueName: 'q1',
            Attributes: {
                VisibilityTimeout: 0
            }
        })
        .then( function ( res ) {
            qurl = res.QueueUrl
            return sqs.sendMessageAsync({
                QueueUrl: qurl,
                MessageBody: 'hello world',
            })
        })
        .then( function () {
            done();
        })
        .catch( done )
    });

    it( 'removes all messages in the queue', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.purgeQueueAsync({
            QueueUrl: qurl
        })
        .then( function () {
            return sqs.receiveMessageAsync({
                QueueUrl: qurl
            })
        })
        .then( function ( res ) {
            assert.equal( res.Messages.length, 0 );
            done();
        })
        .catch( done );
    })

    it( 'fails when queue url is missing', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.purgeQueueAsync({})
            .then( function () {
                done( 'Error expected for required QueueUrl' )
            })
            .catch( function ( err ) {
                assert( err.toString().match( /QueueUrl.*required/ig ) )
                done();
            })
    })

    it( 'fails when queue doesn\'t exists', function ( done ) {
        var sqs = Promise.promisifyAll( new mocksqs.SQS() );
        sqs.purgeQueueAsync({
            QueueUrl: 'hello',
        })
        .then( function () {
            done( 'Error expected for Queue doesn\'t exists' );
        })
        .catch( function ( err ) {
            assert( err.toString().match( /Queue.*doesn\'t exist.*hello/ig ) );
            done();
        })
    })

})













