export function buildMessage(message: any, object: any) {
  for (var attribute in object) {
    message['set' + attribute.charAt(0).toUpperCase() + attribute.slice(1)](object[attribute]);
  }
  return message;
}
