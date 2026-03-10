import httpx


async def request_data(client: httpx.AsyncClient, url: str, parameters: dict):
    try:
        response = await client.get(url, params=parameters)
        response.raise_for_status()
        return response.json()
    except httpx.TimeoutException:
        print("Request timed out!")
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error {exc.response.status_code}: {exc}")
    return None
