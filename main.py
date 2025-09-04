import os
import asyncio
import aiohttp
import math
import statistics
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import re
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherBot:
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        
        # Ubicaciones predefinidas
        self.locations = {
            'S.Jor': (-31.32, -64.34),
            'Cuchi': (-30.99, -64.71),
            'V.Alp': (-32.02, -64.81),
            'Peder': (-31.76, -64.65),
            'N.Pau': (-31.72, -65.00),
            'Merlo': (-32.34, -64.98)
        }
        
        # Modelos meteorológicos con sus URLs base
        self.models = [
            'icon_seamless',
            'gfs_seamless',
            'meteofrance_seamless',
            'ecmwf_ifs025',
            'ukmo_seamless',
            'gem_seamless',
            'cma_grapes_global'
        ]
        
        # Direcciones del viento
        self.wind_directions = [
            "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSO", "SO", "OSO", "O", "ONO", "NO", "NNO"
        ]

    def get_wind_direction(self, degrees: float) -> str:
        """Convierte grados a dirección del viento"""
        # Normalizar entre 0-360
        degrees = degrees % 360
        # Cada dirección cubre 22.5 grados
        index = round(degrees / 22.5) % 16
        return self.wind_directions[index]

    def calculate_dew_point(self, temp: float, humidity: float) -> float:
        """Calcula el punto de rocío usando la fórmula dada"""
        if humidity <= 0:
            humidity = 0.1
        return temp + (35 * math.log10(humidity / 100))

    def calculate_thermal_velocity(self, rocio_termica: float, rocio: float, temp: float) -> float:
        """Calcula la velocidad térmica según la fórmula especificada"""
        try:
            numerator = (1.1 ** abs(rocio_termica - rocio)) - 1
            denominator = 1.1 ** abs(temp - rocio)
            
            if denominator == 0:
                return 0.0
                
            result = 5.6 * math.sqrt(numerator / denominator)
            return round(result, 1)
        except (ValueError, ZeroDivisionError):
            return 0.0

    def build_api_url(self, lat: float, lon: float, model: str) -> str:
        """Construye la URL de la API según el modelo"""
        base_params = f"latitude={lat}&longitude={lon}&timezone=America%2FSao_Paulo&format=json"
        
        if model == 'icon_seamless':
            return f"https://api.open-meteo.com/v1/forecast?{base_params}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,relative_humidity_850hPa,relative_humidity_800hPa,wind_speed_850hPa,wind_speed_800hPa,wind_direction_850hPa,wind_direction_800hPa&models={model}"
        
        elif model == 'ecmwf_ifs025':
            return f"https://api.open-meteo.com/v1/forecast?{base_params}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,relative_humidity_850hPa,wind_speed_850hPa,wind_direction_850hPa&models={model}&forecast_days=7"
        
        else:
            return f"https://api.open-meteo.com/v1/forecast?{base_params}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,temperature_750hPa,relative_humidity_850hPa,relative_humidity_800hPa,relative_humidity_750hPa,wind_speed_850hPa,wind_speed_800hPa,wind_speed_750hPa,wind_direction_850hPa,wind_direction_800hPa,wind_direction_750hPa&models={model}"

    async def fetch_weather_data(self, session: aiohttp.ClientSession, lat: float, lon: float, model: str) -> Optional[Dict]:
        """Obtiene datos meteorológicos de un modelo específico"""
        url = self.build_api_url(lat, lon, model)
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Error {response.status} para modelo {model}")
                    return None
        except Exception as e:
            logger.error(f"Error obteniendo datos del modelo {model}: {e}")
            return None

    async def get_all_weather_data(self, lat: float, lon: float) -> List[Dict]:
        """Obtiene datos de todos los modelos meteorológicos"""
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_weather_data(session, lat, lon, model) for model in self.models]
            results = await asyncio.gather(*tasks)
            return [result for result in results if result is not None]

    def extract_hour_data(self, data: Dict, target_hour: str = "15:00") -> Optional[Dict]:
        """Extrae datos para una hora específica (por defecto 15:00)"""
        try:
            times = data['hourly']['time']
            hourly_data = data['hourly']
            
            # Buscar índice de la hora objetivo para cada día
            daily_data = {}
            for i, time_str in enumerate(times):
                if target_hour in time_str:
                    date = time_str.split('T')[0]
                    if date not in daily_data:
                        daily_data[date] = {}
                        for key, values in hourly_data.items():
                            if key != 'time' and i < len(values):
                                daily_data[date][key] = values[i]
            
            return daily_data
        except Exception as e:
            logger.error(f"Error extrayendo datos por hora: {e}")
            return None

    def calculate_averages_and_thermals(self, all_data: List[Dict], target_date: str) -> Dict:
        """Calcula promedios y valores térmicos para una fecha específica"""
        # Extraer datos para la hora objetivo de todos los modelos
        daily_datasets = []
        for data in all_data:
            extracted = self.extract_hour_data(data)
            if extracted and target_date in extracted:
                daily_datasets.append(extracted[target_date])
        
        if not daily_datasets:
            return {}
        
        # Variables a promediar
        avg_vars = [
            'wind_speed_10m', 'wind_speed_850hPa', 'wind_speed_800hPa', 'wind_speed_750hPa',
            'wind_direction_10m', 'wind_direction_850hPa', 'wind_direction_800hPa', 'wind_direction_750hPa',
            'cloud_cover', 'temperature_2m', 'temperature_850hPa', 'temperature_800hPa', 'temperature_750hPa',
            'relative_humidity_2m', 'relative_humidity_850hPa', 'relative_humidity_800hPa', 'relative_humidity_750hPa'
        ]
        
        result = {}
        
        # Calcular promedios
        for var in avg_vars:
            values = []
            for dataset in daily_datasets:
                if var in dataset and dataset[var] is not None:
                    values.append(float(dataset[var]))
            
            if values:
                result[f'{var}_avg'] = statistics.mean(values)
                result[f'{var}_std'] = statistics.stdev(values) if len(values) > 1 else 0
        
        # Calcular puntos de rocío y térmicas
        thermal_data = {}
        for dataset in daily_datasets:
            # Calcular puntos de rocío para cada altura
            dew_points = {}
            temps = {}
            
            for level in ['2m', '850hPa', '800hPa', '750hPa']:
                temp_key = f'temperature_{level}'
                humidity_key = f'relative_humidity_{level}'
                
                if temp_key in dataset and humidity_key in dataset:
                    temp = float(dataset[temp_key])
                    humidity = float(dataset[humidity_key])
                    dew_points[level] = self.calculate_dew_point(temp, humidity)
                    temps[level] = temp
            
            # Calcular velocidades térmicas
            if '2m' in dew_points:
                rocio_termica = dew_points['2m']
                
                for level in ['850hPa', '800hPa', '750hPa']:
                    if level in dew_points and level in temps:
                        thermal_vel = self.calculate_thermal_velocity(
                            rocio_termica, dew_points[level], temps[level]
                        )
                        
                        if f'thermal_{level}' not in thermal_data:
                            thermal_data[f'thermal_{level}'] = []
                        thermal_data[f'thermal_{level}'].append(thermal_vel)
        
        # Promediar velocidades térmicas
        for key, values in thermal_data.items():
            if values:
                result[f'{key}_avg'] = statistics.mean(values)
                result[f'{key}_std'] = statistics.stdev(values) if len(values) > 1 else 0
        
        return result

    def format_value(self, value: float, width: int, decimals: int = 0) -> str:
        """Formatea un valor con el ancho especificado"""
        if decimals == 0:
            formatted = str(int(round(value)))
        else:
            formatted = f"{value:.{decimals}f}"
        
        return formatted.rjust(width)

    def format_direction(self, degrees: float, width: int) -> str:
        """Formatea una dirección con el ancho especificado"""
        direction = self.get_wind_direction(degrees)
        return direction.ljust(width)

    def format_std_dev(self, value: float, width: int) -> str:
        """Formatea una desviación estándar con ± y el ancho especificado"""
        formatted = f"±{value:.1f}"
        return formatted.rjust(width)

    def generate_forecast_table(self, location_data: Dict[str, Dict], date: datetime, day_name: str) -> str:
        """Genera una tabla de pronóstico para un día específico"""
        date_str = date.strftime("%d %b")
        day_names = {
            0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vie", 5: "sáb", 6: "dom"
        }
        day_abbr = day_names.get(date.weekday(), day_name[:3].lower())
        
        # Encabezado
        table = f"Prono: {day_name} {day_abbr} {date_str} 15hs\n"
        table += "*****|viento km/h      |nb|térmica m/s\n"
        table += "*****| ↓ |1,5k|2k |2,5k|% |1,5k|2k  |2,5k|\n\n"
        
        # Datos para cada ubicación
        for location_name, data in location_data.items():
            if not data:
                continue
                
            # Primera fila: velocidades del viento, nubosidad y térmicas
            row1 = f"{location_name}|"
            row1 += self.format_value(data.get('wind_speed_10m_avg', 0), 3) + "|"
            row1 += self.format_value(data.get('wind_speed_850hPa_avg', 0), 3) + " |"
            row1 += self.format_value(data.get('wind_speed_800hPa_avg', 0), 3) + "|"
            row1 += self.format_value(data.get('wind_speed_750hPa_avg', 0), 3) + " |"
            row1 += self.format_value(data.get('cloud_cover_avg', 0), 2) + "|"
            row1 += self.format_value(data.get('thermal_850hPa_avg', 0), 3, 1) + " |"
            row1 += self.format_value(data.get('thermal_800hPa_avg', 0), 3, 1) + " |"
            row1 += self.format_value(data.get('thermal_750hPa_avg', 0), 3, 1) + " |"
            
            # Segunda fila: direcciones del viento y desviaciones estándar
            row2 = f"{location_name}|"
            row2 += self.format_direction(data.get('wind_direction_10m_avg', 0), 3) + "|"
            row2 += self.format_direction(data.get('wind_direction_850hPa_avg', 0), 3) + " |"
            row2 += self.format_direction(data.get('wind_direction_800hPa_avg', 0), 3) + "|"
            row2 += self.format_direction(data.get('wind_direction_750hPa_avg', 0), 3) + " |"
            row2 += "% |"
            row2 += self.format_std_dev(data.get('thermal_850hPa_std', 0), 4) + "|"
            row2 += self.format_std_dev(data.get('thermal_800hPa_std', 0), 4) + "|"
            row2 += self.format_std_dev(data.get('thermal_750hPa_std', 0), 4) + "|"
            
            table += row1 + "\n" + row2 + "\n\n"
        
        return table.strip()

    async def generate_forecasts_for_locations(self, locations: Dict[str, Tuple[float, float]]) -> List[str]:
        """Genera pronósticos para múltiples ubicaciones y 4 días"""
        forecasts = []
        
        # Obtener fecha base (hoy)
        base_date = datetime.now()
        day_names = ["hoy", "mañ", "pas mañ", "en 3 días"]
        
        for day_offset in range(4):
            target_date = base_date + timedelta(days=day_offset)
            date_str = target_date.strftime("%Y-%m-%d")
            
            location_data = {}
            
            for location_name, (lat, lon) in locations.items():
                # Obtener datos de todos los modelos para esta ubicación
                all_data = await self.get_all_weather_data(lat, lon)
                
                # Calcular promedios y térmicas para la fecha objetivo
                data = self.calculate_averages_and_thermals(all_data, date_str)
                location_data[location_name] = data
            
            # Generar tabla para este día
            table = self.generate_forecast_table(location_data, target_date, day_names[day_offset])
            forecasts.append(table)
        
        return forecasts

    def parse_coordinates(self, message: str) -> Optional[Tuple[float, float]]:
        """Parsea coordenadas del mensaje"""
        # Buscar patrón de coordenadas: lat, lon
        pattern = r'(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern, message)
        
        if match:
            try:
                lat = float(match.group(1))
                lon = float(match.group(2))
                return (lat, lon)
            except ValueError:
                return None
        return None

    async def send_message(self, chat_id: int, text: str):
        """Envía un mensaje a través del bot de Telegram"""
        url = f"{self.base_url}/sendMessage"
        data = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'Markdown'
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=data) as response:
                    if response.status != 200:
                        logger.error(f"Error enviando mensaje: {response.status}")
            except Exception as e:
                logger.error(f"Error enviando mensaje: {e}")

    async def handle_update(self, update: Dict):
        """Maneja actualizaciones del bot"""
        if 'message' not in update:
            return
            
        message = update['message']
        chat_id = message['chat']['id']
        text = message.get('text', '').strip()
        
        if not text:
            return
        
        try:
            # Verificar si son coordenadas
            coords = self.parse_coordinates(text)
            
            if coords:
                lat, lon = coords
                # Crear ubicación personalizada
                custom_locations = {
                    f"{lat:.1f}": (lat, lon)
                }
                forecasts = await self.generate_forecasts_for_locations(custom_locations)
            else:
                # Usar ubicaciones predefinidas
                forecasts = await self.generate_forecasts_for_locations(self.locations)
            
            # Enviar cada pronóstico como mensaje separado
            for forecast in forecasts:
                await self.send_message(chat_id, f"```\n{forecast}\n```")
                await asyncio.sleep(1)  # Evitar rate limiting
                
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            await self.send_message(chat_id, "Error procesando el pronóstico. Intenta nuevamente.")

    async def get_updates(self, offset: int = 0) -> List[Dict]:
        """Obtiene actualizaciones del bot"""
        url = f"{self.base_url}/getUpdates"
        params = {'offset': offset, 'timeout': 30}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('result', [])
                    else:
                        logger.error(f"Error obteniendo updates: {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error obteniendo updates: {e}")
                return []

    async def run(self):
        """Ejecuta el bot en modo polling"""
        logger.info("Iniciando bot meteorológico...")
        offset = 0
        
        while True:
            try:
                updates = await self.get_updates(offset)
                
                for update in updates:
                    offset = update['update_id'] + 1
                    await self.handle_update(update)
                
                if not updates:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error en el loop principal: {e}")
                await asyncio.sleep(5)

# Servidor web para Render.com
from aiohttp import web

async def health_check(request):
    """Endpoint de health check para Render"""
    return web.Response(text="OK")

async def init_app():
    """Inicializa la aplicación web"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    return app

async def main():
    """Función principal"""
    # Obtener token del bot desde variable de entorno
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN no configurado")
        return
    
    logger.info(f"Iniciando con token: {bot_token[:10]}...")
    
    # Crear instancia del bot
    bot = WeatherBot(bot_token)
    
    # Inicializar servidor web para Render
    app = await init_app()
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    logger.info(f"Servidor web iniciando en puerto {port}")
    await site.start()
    
    # Ejecutar solo el bot (el servidor ya está iniciado)
    await bot.run()

if __name__ == '__main__':
    asyncio.run(main())
