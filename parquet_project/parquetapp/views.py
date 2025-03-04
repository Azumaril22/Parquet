import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import ParquetUser


def user_list(request):
    users = ParquetUser.all()
    return JsonResponse([user.to_dict() for user in users], safe=False)


@csrf_exempt
def add_user(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body.decode('utf-8'))
            id = int(data['id'])
            username = data['username']
            email = data['email']

            user = ParquetUser(id=id, username=username, email=email)
            ParquetUser.save(user)

            return JsonResponse({"message": "User saved"}, status=201)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            return JsonResponse({"error": f"Invalid data: {e}"}, status=400)

    return JsonResponse({"error": "Invalid method"}, status=405)

""" 
exemple POST :
{
  "id": "1",
  "username": "alice",
  "email": "alice@example.com"
}
"""