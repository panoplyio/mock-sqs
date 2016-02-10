var extend = require( 'extend' );
var uuid = require( 'node-uuid' );

var queues = {};
module.exports = SQS;
module.exports.reset = function () {
    queues = {};
}

function SQS ( options ) {
    options = options || {};
    this.config = extend({
        region: 'us-east-1',
    }, options )
    this.params = options.params || {};
}

SQS.prototype.createQueue = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );

    var qname = params.QueueName;
    if ( !params.QueueName ) {
        callback( new Error( 'QueueName is required' ) );
        return;
    }

    var region = this.config.region;
    var qurl = [
        'https://sqs.', region, '.amazonaws.com/123456789/', qname
    ].join( '' ); 
    
    if ( queues[ qurl ] ) {
        callback( new Error( 'Queue "' + qname + '" already exists' ) );
        return;
    }

    var attributes = extend({
        DelaySeconds: 0,
        MaximumMessageSize: 262144,
        MessageRetentionPeriod: 345600,
        ReceiveMessageWaitTimeSeconds: 0,
        VisibilityTimeout: 30
    }, params.Attributes );

    queues[ qurl ] = {
        attributes: attributes,
        messages: []
    }

    setTimeout( function () {
        callback( null, {
            QueueUrl: qurl
        })
    })
}

SQS.prototype.deleteQueue = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );

    var qurl = params.QueueUrl;
    delete queues[ qurl ];

    setTimeout( function () {
        callback( null, {} )
    })
}

SQS.prototype.listQueues = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );

    var prefix = params.QueueNamePrefix || '';
    var urls = Object.keys( queues )
        .filter( function ( qurl ) {
            return qurl.indexOf( prefix ) === 0;
        })

    setTimeout( function () {
        callback( null, {
            QueueUrls: urls
        })
    })
}

SQS.prototype.purgeQueue = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );

    var qurl = params.QueueUrl;
    if ( !qurl ) {
        callback( new Error( 'QueueUrl is required' ) )
        return
    }

    var queue = queues[ qurl ];
    if ( !queue ) {
        callback( new Error( 'Queue doesn\'t exist for Url:' + qurl ) );
        return;
    }

    queue.messages = [];

    setTimeout( function () {
        callback( null, {} )
    })
}

SQS.prototype.sendMessage = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );

    var qurl = params.QueueUrl;
    if ( !qurl ) {
        callback( new Error( 'QueueUrl is required' ) )
        return
    }

    var body = params.MessageBody;
    if ( !body ) {
        callback( new Error( 'MessageBody is required' ) );
        return;
    }

    var queue = queues[ qurl ];
    if ( !queue ) {
        callback( new Error( 'Queue doesn\'t exist for Url:' + qurl ) );
        return;
    }

    var attributes = params.MessageAttributes;
    var messageId = uuid.v4();
    queue.messages.push({
        MessageId: messageId,
        Body: body,
        MessageAttributes: extend( {}, attributes ) // copy
    })

    setTimeout( function () {
        callback( null, {
            MessageId: messageId,
        })
    })
}

SQS.prototype.receiveMessage = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );
    var qurl = params.QueueUrl;
    if ( !qurl ) {
        callback( new Error( 'QueueUrl is required' ) );
        return;
    }

    var queue = queues[ qurl ];
    if ( !queue ) {
        callback( new Error( 'Queue doesn\'t exist for Url:' + qurl ) );
        return;
    }

    var max = params.MaxNumberOfMessages || 1;
    if ( max > 10 || max < 1 ) {
        callback( new Error( 'MaxNumberOfMessages out of range' ) );
        return;
    }

    var messages = queue.messages;
    var received = messages.slice( 0, max )
        .map( function ( message ) {
            message.ReceiptHandle = uuid.v4()
            return message;
        });

    setTimeout( function () {
        callback( null, {
            Messages: received
        })
    })
}

SQS.prototype.deleteMessage = function ( params, callback ) {
    callback = callback || function () {}
    params = extend( {}, this.params, params );
    var qurl = params.QueueUrl;
    if ( !qurl ) {
        callback( new Error( 'QueueUrl is required' ) );
        return;
    }

    var receipt = params.ReceiptHandle;
    if ( !receipt ) {
        callback( new Error( 'ReceiptHandle is required' ) );
        return;
    }

    var queue = queues[ qurl ];
    if ( !queue ) {
        callback( new Error( 'Queue doesn\'t exist for Url:' + qurl ) );
        return;
    }

    queue.messages = queue.messages.filter( function ( message ) {
        return message.ReceiptHandle !== receipt;
    })

    setTimeout( function () {
        callback( null, {} )
    })
}


