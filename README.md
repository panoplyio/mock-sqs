# mock-sqs
Mocking library for Amazon's SQS Node.js SDK

**IMPORTANT**: This project is a work in progress, and is not yet
a complete rewrite of the entire SQS SDK. We add feature support as 
needed. Any contributions are welcome.

#### Usage

```javascript
var mocksqs = require( 'mock-sqs' );

before( function () {
    sinon.stub( aws, 'SQS', mocksqs.SQS );
})

after( function () {
    aws.SQS.restore();
})
```